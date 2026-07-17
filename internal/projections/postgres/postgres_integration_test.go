//go:build integration

// See internal/eventstore/postgres/postgres_integration_test.go for how to
// run this file locally; it is excluded from the default (offline) test
// suite by the "integration" build tag.
package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"streamledger/internal/projections"
	"streamledger/internal/projections/postgres"
)

func requireDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres integration test")
	}
	return url
}

func newTestStore(t *testing.T) *postgres.Store {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, requireDatabaseURL(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	store := postgres.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE accounts_view, inventory_view, projection_checkpoints`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store
}

func TestPostgresStore_UpsertAndGetAccount(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	view := projections.AccountView{AccountID: "acct-1", BalanceCents: 500, Version: 3, UpdatedAt: time.Now().UTC().Truncate(time.Microsecond)}
	if err := store.UpsertAccount(ctx, view); err != nil {
		t.Fatalf("UpsertAccount: %v", err)
	}

	got, found, err := store.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if !found || got.BalanceCents != 500 || got.Version != 3 {
		t.Fatalf("GetAccount = %+v found=%v, want balance=500 version=3 found=true", got, found)
	}
}

func TestPostgresStore_CheckpointMonotonic(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	if err := store.SetCheckpoint(ctx, "balances", 10); err != nil {
		t.Fatalf("SetCheckpoint: %v", err)
	}
	// Attempting to move it backwards must be a no-op.
	if err := store.SetCheckpoint(ctx, "balances", 3); err != nil {
		t.Fatalf("SetCheckpoint (regress): %v", err)
	}
	got, err := store.GetCheckpoint(ctx, "balances")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	if got != 10 {
		t.Fatalf("checkpoint = %d, want 10 (must never move backwards)", got)
	}
}

func TestPostgresStore_WithTxRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	testErr := context.Canceled // any sentinel error works here
	err := store.WithTx(ctx, func(ctx context.Context) error {
		if err := store.UpsertAccount(ctx, projections.AccountView{AccountID: "acct-1", BalanceCents: 999, Version: 1, UpdatedAt: time.Now()}); err != nil {
			return err
		}
		return testErr
	})
	if err != testErr {
		t.Fatalf("WithTx err = %v, want %v", err, testErr)
	}

	_, found, err := store.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if found {
		t.Fatal("expected the upsert to be rolled back with the rest of the failed transaction")
	}
}
