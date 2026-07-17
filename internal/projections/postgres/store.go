// Package postgres is the production projections.ReadModelStore
// implementation, backing the materialized accounts_view/inventory_view
// tables defined in schema/0001_views.sql. It is exercised by an optional,
// opt-in integration test (build tag "integration") rather than the
// default unit test suite — see postgres_integration_test.go — because
// this project's offline-verifiable contract is "no network, no running
// database at test time". internal/projections/memory stands in for it in
// every table-driven test that proves idempotent-apply and projection
// correctness, since those are properties of Projector and the
// EventApplier implementations, not of which store backs ReadModelStore.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"streamledger/internal/projections"
)

//go:embed schema/*.sql
var schemaFS embed.FS

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store is a Postgres-backed projections.ReadModelStore.
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens a pgxpool against databaseURL and pings it.
func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("projections/postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("projections/postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// New wraps an already-constructed pool (used by tests that build the pool
// themselves, e.g. against a real database reachable via DATABASE_URL).
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate applies every embedded schema file, in filename order. Every
// statement is IF NOT EXISTS, so it is safe to call on every process start.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := schemaFS.ReadDir("schema")
	if err != nil {
		return fmt.Errorf("projections/postgres: read schema dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		b, err := schemaFS.ReadFile("schema/" + name)
		if err != nil {
			return fmt.Errorf("projections/postgres: read %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("projections/postgres: apply %s: %w", name, err)
		}
	}
	return nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// txKey is the context sentinel WithTx uses to hand its transaction down
// to nested ReadModelStore calls made from inside fn.
type txKey struct{}

func withTx(ctx context.Context, tx querier) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func (s *Store) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(querier); ok {
		return tx
	}
	return s.pool
}

// WithTx runs fn inside a real SQL transaction: every ReadModelStore call
// fn makes via ctx participates in it, and it commits only if fn returns
// nil. This is the mechanism that makes a Projector's
// check-checkpoint/mutate-view/advance-checkpoint sequence atomic in
// production — a crash or error partway through leaves neither the view
// nor the checkpoint changed, so a retry is always a clean redo, never a
// double-count.
func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("projections/postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	if err := fn(withTx(ctx, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("projections/postgres: commit: %w", err)
	}
	return nil
}

func (s *Store) GetAccount(ctx context.Context, accountID string) (projections.AccountView, bool, error) {
	var v projections.AccountView
	v.AccountID = accountID
	err := s.q(ctx).QueryRow(ctx,
		`SELECT balance_cents, version, updated_at FROM accounts_view WHERE account_id = $1`,
		accountID,
	).Scan(&v.BalanceCents, &v.Version, &v.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return projections.AccountView{}, false, nil
		}
		return projections.AccountView{}, false, fmt.Errorf("projections/postgres: get account: %w", err)
	}
	return v, true, nil
}

func (s *Store) UpsertAccount(ctx context.Context, view projections.AccountView) error {
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO accounts_view (account_id, balance_cents, version, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_id) DO UPDATE
		SET balance_cents = EXCLUDED.balance_cents,
		    version = EXCLUDED.version,
		    updated_at = EXCLUDED.updated_at`,
		view.AccountID, view.BalanceCents, view.Version, view.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("projections/postgres: upsert account: %w", err)
	}
	return nil
}

func (s *Store) GetInventory(ctx context.Context, itemID string) (projections.InventoryView, bool, error) {
	var v projections.InventoryView
	v.ItemID = itemID
	err := s.q(ctx).QueryRow(ctx,
		`SELECT available_quantity, reserved_quantity, version, updated_at FROM inventory_view WHERE item_id = $1`,
		itemID,
	).Scan(&v.AvailableQuantity, &v.ReservedQuantity, &v.Version, &v.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return projections.InventoryView{}, false, nil
		}
		return projections.InventoryView{}, false, fmt.Errorf("projections/postgres: get inventory: %w", err)
	}
	return v, true, nil
}

func (s *Store) UpsertInventory(ctx context.Context, view projections.InventoryView) error {
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO inventory_view (item_id, available_quantity, reserved_quantity, version, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (item_id) DO UPDATE
		SET available_quantity = EXCLUDED.available_quantity,
		    reserved_quantity = EXCLUDED.reserved_quantity,
		    version = EXCLUDED.version,
		    updated_at = EXCLUDED.updated_at`,
		view.ItemID, view.AvailableQuantity, view.ReservedQuantity, view.Version, view.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("projections/postgres: upsert inventory: %w", err)
	}
	return nil
}

func (s *Store) GetCheckpoint(ctx context.Context, projectionName string) (int64, error) {
	var seq int64
	err := s.q(ctx).QueryRow(ctx,
		`SELECT global_seq FROM projection_checkpoints WHERE projection_name = $1`,
		projectionName,
	).Scan(&seq)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("projections/postgres: get checkpoint: %w", err)
	}
	return seq, nil
}

// SetCheckpoint advances the checkpoint monotonically: the GREATEST(...)
// makes the write itself ignore any attempt to move the checkpoint
// backwards, in one atomic statement, without needing a separate
// SELECT ... FOR UPDATE round-trip.
func (s *Store) SetCheckpoint(ctx context.Context, projectionName string, globalSeq int64) error {
	_, err := s.q(ctx).Exec(ctx, `
		INSERT INTO projection_checkpoints (projection_name, global_seq)
		VALUES ($1, $2)
		ON CONFLICT (projection_name) DO UPDATE
		SET global_seq = GREATEST(projection_checkpoints.global_seq, EXCLUDED.global_seq)`,
		projectionName, globalSeq,
	)
	if err != nil {
		return fmt.Errorf("projections/postgres: set checkpoint: %w", err)
	}
	return nil
}

var _ projections.ReadModelStore = (*Store)(nil)
