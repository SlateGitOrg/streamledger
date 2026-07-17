// Package grpcserver implements the gRPC command/query API defined in
// proto/ledger/v1/ledger.proto on top of internal/ledger (commands) and
// internal/projections (queries).
package grpcserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerv1 "streamledger/gen/ledger/v1"
	"streamledger/internal/eventstore"
	"streamledger/internal/ledger"
	"streamledger/internal/projections"
)

// Server implements ledgerv1.LedgerServiceServer.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer

	Ledger     *ledger.Service
	EventStore eventstore.Store
	Balances   *projections.Projector
	Inventory  *projections.Projector
	ReadModels projections.ReadModelStore
}

// New constructs a Server. Every dependency is required.
func New(svc *ledger.Service, store eventstore.Store, balances, inventory *projections.Projector, readModels projections.ReadModelStore) *Server {
	return &Server{
		Ledger:     svc,
		EventStore: store,
		Balances:   balances,
		Inventory:  inventory,
		ReadModels: readModels,
	}
}

var _ ledgerv1.LedgerServiceServer = (*Server)(nil)

// mapError translates domain/eventstore errors into gRPC status errors with
// codes a client can branch on programmatically.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, eventstore.ErrVersionConflict):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount),
		errors.Is(err, ledger.ErrInvalidQuantity),
		errors.Is(err, ledger.ErrSameAccountTransfer):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ledger.ErrAccountAlreadyOpened):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ledger.ErrAccountNotOpen),
		errors.Is(err, ledger.ErrInsufficientFunds),
		errors.Is(err, ledger.ErrInsufficientStock):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func toCommandResponse(res ledger.Result, err error) (*ledgerv1.CommandResponse, error) {
	if err != nil {
		return nil, mapError(err)
	}
	msg := "applied"
	if res.Duplicate {
		msg = "duplicate: replaying cached result"
	}
	return &ledgerv1.CommandResponse{
		Applied:    res.Applied,
		Duplicate:  res.Duplicate,
		NewVersion: res.NewVersion,
		Message:    msg,
	}, nil
}

func (s *Server) OpenAccount(ctx context.Context, req *ledgerv1.OpenAccountRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	res, err := s.Ledger.OpenAccount(ctx, req.GetAccountId(), req.GetInitialBalanceCents(), req.GetDedupeKey())
	return toCommandResponse(res, err)
}

func (s *Server) Deposit(ctx context.Context, req *ledgerv1.DepositRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	res, err := s.Ledger.Deposit(ctx, req.GetAccountId(), req.GetAmountCents(), "", req.GetDedupeKey(), expectedVersionOrAuto(req.GetExpectedVersion()))
	return toCommandResponse(res, err)
}

func (s *Server) Withdraw(ctx context.Context, req *ledgerv1.WithdrawRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	res, err := s.Ledger.Withdraw(ctx, req.GetAccountId(), req.GetAmountCents(), "", req.GetDedupeKey(), expectedVersionOrAuto(req.GetExpectedVersion()))
	return toCommandResponse(res, err)
}

func (s *Server) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetFromAccountId() == "" || req.GetToAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account_id and to_account_id are required")
	}
	res, err := s.Ledger.Transfer(ctx, req.GetFromAccountId(), req.GetToAccountId(), req.GetAmountCents(), req.GetDedupeKey())
	return toCommandResponse(res, err)
}

func (s *Server) AdjustInventory(ctx context.Context, req *ledgerv1.AdjustInventoryRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetItemId() == "" {
		return nil, status.Error(codes.InvalidArgument, "item_id is required")
	}
	res, err := s.Ledger.AdjustInventory(ctx, req.GetItemId(), req.GetDeltaQuantity(), req.GetReason(), req.GetDedupeKey(), eventstore.NoVersionCheck)
	return toCommandResponse(res, err)
}

func (s *Server) ReserveStock(ctx context.Context, req *ledgerv1.ReserveStockRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetItemId() == "" {
		return nil, status.Error(codes.InvalidArgument, "item_id is required")
	}
	res, err := s.Ledger.ReserveStock(ctx, req.GetItemId(), req.GetQuantity(), req.GetOrderId(), req.GetDedupeKey(), eventstore.NoVersionCheck)
	return toCommandResponse(res, err)
}

func (s *Server) ReleaseStock(ctx context.Context, req *ledgerv1.ReleaseStockRequest) (*ledgerv1.CommandResponse, error) {
	if req.GetItemId() == "" {
		return nil, status.Error(codes.InvalidArgument, "item_id is required")
	}
	res, err := s.Ledger.ReleaseStock(ctx, req.GetItemId(), req.GetQuantity(), req.GetOrderId(), req.GetDedupeKey(), eventstore.NoVersionCheck)
	return toCommandResponse(res, err)
}

func (s *Server) GetBalance(ctx context.Context, req *ledgerv1.GetBalanceRequest) (*ledgerv1.GetBalanceResponse, error) {
	if req.GetAccountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "account_id is required")
	}
	view, found, err := s.ReadModels.GetAccount(ctx, req.GetAccountId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &ledgerv1.GetBalanceResponse{
		AccountId:       req.GetAccountId(),
		Found:           found,
		BalanceCents:    view.BalanceCents,
		Version:         view.Version,
		UpdatedAtUnixMs: view.UpdatedAt.UnixMilli(),
	}, nil
}

func (s *Server) GetInventory(ctx context.Context, req *ledgerv1.GetInventoryRequest) (*ledgerv1.GetInventoryResponse, error) {
	if req.GetItemId() == "" {
		return nil, status.Error(codes.InvalidArgument, "item_id is required")
	}
	view, found, err := s.ReadModels.GetInventory(ctx, req.GetItemId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &ledgerv1.GetInventoryResponse{
		ItemId:            req.GetItemId(),
		Found:             found,
		AvailableQuantity: view.AvailableQuantity,
		ReservedQuantity:  view.ReservedQuantity,
		Version:           view.Version,
		UpdatedAtUnixMs:   view.UpdatedAt.UnixMilli(),
	}, nil
}

func (s *Server) GetProjectionLag(ctx context.Context, _ *ledgerv1.GetProjectionLagRequest) (*ledgerv1.GetProjectionLagResponse, error) {
	latest, err := s.EventStore.LatestGlobalSeq(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &ledgerv1.GetProjectionLagResponse{}
	for _, p := range []*projections.Projector{s.Balances, s.Inventory} {
		checkpoint, err := p.Checkpoint(ctx)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		lag := latest - checkpoint
		if lag < 0 {
			lag = 0
		}
		resp.Lags = append(resp.Lags, &ledgerv1.ProjectionLag{
			ProjectionName:   p.Name,
			LatestGlobalSeq:  latest,
			AppliedGlobalSeq: checkpoint,
			Lag:              lag,
		})
	}
	return resp, nil
}

// expectedVersionOrAuto maps the proto's expected_version convention onto
// eventstore's, which happen to already agree (-1 == no check), but this
// indirection keeps the wire contract decoupled from the internal sentinel.
func expectedVersionOrAuto(v int64) int64 {
	if v < 0 {
		return eventstore.NoVersionCheck
	}
	return v
}
