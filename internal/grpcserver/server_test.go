package grpcserver_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "streamledger/gen/ledger/v1"
	"streamledger/internal/eventstore"
	eventmemory "streamledger/internal/eventstore/memory"
	"streamledger/internal/grpcserver"
	"streamledger/internal/ledger"
	"streamledger/internal/projections"
	projmemory "streamledger/internal/projections/memory"
)

// newTestServer wires the same components cmd/streamledger/main.go does,
// entirely in-memory, and immediately catches up both projections so
// queries reflect commands issued moments earlier in the same test.
func newTestServer(t *testing.T) (*grpcserver.Server, *eventmemory.Store, *projections.Projector, *projections.Projector) {
	t.Helper()
	esStore := eventmemory.New()
	rmStore := projmemory.New()
	svc := ledger.NewService(esStore, nil)
	balances := projections.NewProjector("balances", rmStore, &projections.AccountApplier{Store: rmStore}, nil)
	inventory := projections.NewProjector("inventory", rmStore, &projections.InventoryApplier{Store: rmStore}, nil)
	return grpcserver.New(svc, esStore, balances, inventory, rmStore), esStore, balances, inventory
}

func catchUp(t *testing.T, ctx context.Context, store eventstore.Store, projectors ...*projections.Projector) {
	t.Helper()
	for _, p := range projectors {
		if _, err := p.CatchUp(ctx, store, 100); err != nil {
			t.Fatalf("CatchUp(%s): %v", p.Name, err)
		}
	}
}

func TestServer_OpenDepositQueryRoundTrip(t *testing.T) {
	ctx := context.Background()
	srv, esStore, balances, inventory := newTestServer(t)

	if _, err := srv.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{AccountId: "acct-1", InitialBalanceCents: 1000}); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	if _, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: 250, ExpectedVersion: -1}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	catchUp(t, ctx, esStore, balances, inventory)

	resp, err := srv.GetBalance(ctx, &ledgerv1.GetBalanceRequest{AccountId: "acct-1"})
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !resp.GetFound() || resp.GetBalanceCents() != 1250 {
		t.Fatalf("GetBalance = %+v, want found=true balance=1250", resp)
	}
}

func TestServer_GetBalance_NotFound(t *testing.T) {
	ctx := context.Background()
	srv, _, _, _ := newTestServer(t)

	resp, err := srv.GetBalance(ctx, &ledgerv1.GetBalanceRequest{AccountId: "ghost"})
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if resp.GetFound() {
		t.Fatal("expected found=false for an account that never existed")
	}
}

func TestServer_ErrorCodeMapping(t *testing.T) {
	ctx := context.Background()
	srv, _, _, _ := newTestServer(t)

	if _, err := srv.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{AccountId: "acct-1", InitialBalanceCents: 10}); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	tests := []struct {
		name     string
		call     func() error
		wantCode codes.Code
	}{
		{
			name: "insufficient funds -> FailedPrecondition",
			call: func() error {
				_, err := srv.Withdraw(ctx, &ledgerv1.WithdrawRequest{AccountId: "acct-1", AmountCents: 999999, ExpectedVersion: -1})
				return err
			},
			wantCode: codes.FailedPrecondition,
		},
		{
			name: "invalid amount -> InvalidArgument",
			call: func() error {
				_, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: -5, ExpectedVersion: -1})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "reopening an account -> AlreadyExists",
			call: func() error {
				_, err := srv.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{AccountId: "acct-1", InitialBalanceCents: 10})
				return err
			},
			wantCode: codes.AlreadyExists,
		},
		{
			name: "stale expected_version -> Aborted",
			call: func() error {
				_, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: 5, ExpectedVersion: 999})
				return err
			},
			wantCode: codes.Aborted,
		},
		{
			name: "missing account_id -> InvalidArgument",
			call: func() error {
				_, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AmountCents: 5, ExpectedVersion: -1})
				return err
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("error %v is not a gRPC status error", err)
			}
			if st.Code() != tc.wantCode {
				t.Fatalf("code = %v, want %v (message: %s)", st.Code(), tc.wantCode, st.Message())
			}
		})
	}
}

func TestServer_DedupeKeyReturnsCachedResponse(t *testing.T) {
	ctx := context.Background()
	srv, _, _, _ := newTestServer(t)

	if _, err := srv.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{AccountId: "acct-1", InitialBalanceCents: 0}); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	first, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: 100, DedupeKey: "req-1", ExpectedVersion: -1})
	if err != nil {
		t.Fatalf("first Deposit: %v", err)
	}
	if !first.GetApplied() || first.GetDuplicate() {
		t.Fatalf("first Deposit = %+v, want Applied=true Duplicate=false", first)
	}

	second, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: 100, DedupeKey: "req-1", ExpectedVersion: -1})
	if err != nil {
		t.Fatalf("second Deposit: %v", err)
	}
	if second.GetApplied() || !second.GetDuplicate() || second.GetNewVersion() != first.GetNewVersion() {
		t.Fatalf("second Deposit = %+v, want a duplicate replay of %+v", second, first)
	}
}

func TestServer_InventoryCommandsAndQuery(t *testing.T) {
	ctx := context.Background()
	srv, esStore, balances, inventory := newTestServer(t)

	if _, err := srv.AdjustInventory(ctx, &ledgerv1.AdjustInventoryRequest{ItemId: "item-1", DeltaQuantity: 100}); err != nil {
		t.Fatalf("AdjustInventory: %v", err)
	}
	if _, err := srv.ReserveStock(ctx, &ledgerv1.ReserveStockRequest{ItemId: "item-1", Quantity: 40, OrderId: "order-1"}); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}
	catchUp(t, ctx, esStore, balances, inventory)

	resp, err := srv.GetInventory(ctx, &ledgerv1.GetInventoryRequest{ItemId: "item-1"})
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if resp.GetAvailableQuantity() != 60 || resp.GetReservedQuantity() != 40 {
		t.Fatalf("GetInventory = %+v, want available=60 reserved=40", resp)
	}

	_, err = srv.ReserveStock(ctx, &ledgerv1.ReserveStockRequest{ItemId: "item-1", Quantity: 1000, OrderId: "order-2"})
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("over-reservation code = %v, want FailedPrecondition", st.Code())
	}
}

func TestServer_GetProjectionLag(t *testing.T) {
	ctx := context.Background()
	srv, esStore, balances, inventory := newTestServer(t)

	if _, err := srv.OpenAccount(ctx, &ledgerv1.OpenAccountRequest{AccountId: "acct-1"}); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	if _, err := srv.Deposit(ctx, &ledgerv1.DepositRequest{AccountId: "acct-1", AmountCents: 10, ExpectedVersion: -1}); err != nil {
		t.Fatalf("Deposit: %v", err)
	}

	before, err := srv.GetProjectionLag(ctx, &ledgerv1.GetProjectionLagRequest{})
	if err != nil {
		t.Fatalf("GetProjectionLag: %v", err)
	}
	var sawLag bool
	for _, l := range before.GetLags() {
		if l.GetLag() > 0 {
			sawLag = true
		}
	}
	if !sawLag {
		t.Fatal("expected at least one projection to report nonzero lag before catch-up")
	}

	catchUp(t, ctx, esStore, balances, inventory)

	after, err := srv.GetProjectionLag(ctx, &ledgerv1.GetProjectionLagRequest{})
	if err != nil {
		t.Fatalf("GetProjectionLag: %v", err)
	}
	for _, l := range after.GetLags() {
		if l.GetLag() != 0 {
			t.Fatalf("projection %s lag = %d after catch-up, want 0", l.GetProjectionName(), l.GetLag())
		}
	}
}
