// Package postgres is the production eventstore.Store implementation. Each
// Append call runs in a single serializable-enough transaction (row locks
// via SELECT ... FOR UPDATE on stream_versions) so that concurrent writers
// to the same aggregate are safely serialized by Postgres itself rather
// than by anything in-process — the actual mechanism that makes StreamLedger
// race-condition-free under concurrent load, not just under a single
// Go process's mutex.
//
// It is exercised by an optional Testcontainers integration test
// (postgres_integration_test.go, build tag "integration") rather than by
// the default unit test suite, per this project's offline-verifiable,
// no-Docker-required contract.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"streamledger/internal/eventstore"
)

//go:embed schema/*.sql
var schemaFS embed.FS

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, letting the
// read-only query helpers below run either standalone or inside a
// transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store is a Postgres-backed eventstore.Store.
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens a pgxpool against databaseURL and pings it.
func Connect(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("postgres store: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres store: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// New wraps an already-constructed pool (used by tests that build the pool
// themselves, e.g. against a Testcontainers instance).
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Migrate applies every embedded schema file, in filename order. It is
// idempotent (every statement is IF NOT EXISTS) so it's safe to call on
// every process start.
func (s *Store) Migrate(ctx context.Context) error {
	entries, err := schemaFS.ReadDir("schema")
	if err != nil {
		return fmt.Errorf("postgres store: read schema dir: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		b, err := schemaFS.ReadFile("schema/" + name)
		if err != nil {
			return fmt.Errorf("postgres store: read %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("postgres store: apply %s: %w", name, err)
		}
	}
	return nil
}

// Close releases the underlying connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

func scanEvent(row pgx.Row) (eventstore.Event, error) {
	var ev eventstore.Event
	var dedupe, correlation *string
	if err := row.Scan(
		&ev.GlobalSeq, &ev.AggregateType, &ev.AggregateID, &ev.Version,
		&ev.EventType, &ev.Payload, &dedupe, &correlation, &ev.CreatedAt,
	); err != nil {
		return eventstore.Event{}, err
	}
	if dedupe != nil {
		ev.Metadata.DedupeKey = *dedupe
	}
	if correlation != nil {
		ev.Metadata.CorrelationID = *correlation
	}
	return ev, nil
}

const eventColumns = `global_seq, aggregate_type, aggregate_id, version, event_type, payload, dedupe_key, correlation_id, created_at`

func loadDedupeEvents(ctx context.Context, q querier, dedupeKey string) ([]eventstore.Event, error) {
	rows, err := q.Query(ctx, `
		SELECT e.global_seq, e.aggregate_type, e.aggregate_id, e.version, e.event_type,
		       e.payload, e.dedupe_key, e.correlation_id, e.created_at
		FROM idempotency_key_events ike
		JOIN events e ON e.global_seq = ike.global_seq
		WHERE ike.dedupe_key = $1
		ORDER BY e.global_seq`, dedupeKey)
	if err != nil {
		return nil, fmt.Errorf("postgres store: query dedupe events: %w", err)
	}
	defer rows.Close()

	var out []eventstore.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres store: scan dedupe event: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// Append implements eventstore.Store.Append. See the package doc for the
// concurrency-control strategy.
func (s *Store) Append(ctx context.Context, dedupeKey string, toAppend []eventstore.AppendEvent) ([]eventstore.Event, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("postgres store: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	if dedupeKey != "" {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM idempotency_keys WHERE dedupe_key = $1 FOR UPDATE)`, dedupeKey).Scan(&exists); err != nil {
			return nil, false, fmt.Errorf("postgres store: check dedupe: %w", err)
		}
		if exists {
			events, err := loadDedupeEvents(ctx, tx, dedupeKey)
			if err != nil {
				return nil, false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return nil, false, fmt.Errorf("postgres store: commit dedupe read: %w", err)
			}
			return events, true, nil
		}
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (dedupe_key) VALUES ($1)`, dedupeKey); err != nil {
			return nil, false, fmt.Errorf("postgres store: reserve dedupe key: %w", err)
		}
	}

	produced := make([]eventstore.Event, 0, len(toAppend))
	for _, e := range toAppend {
		if _, err := tx.Exec(ctx,
			`INSERT INTO stream_versions (aggregate_type, aggregate_id, version) VALUES ($1, $2, 0)
			 ON CONFLICT (aggregate_type, aggregate_id) DO NOTHING`,
			e.AggregateType, e.AggregateID,
		); err != nil {
			return nil, false, fmt.Errorf("postgres store: ensure stream row: %w", err)
		}

		var current int64
		if err := tx.QueryRow(ctx,
			`SELECT version FROM stream_versions WHERE aggregate_type = $1 AND aggregate_id = $2 FOR UPDATE`,
			e.AggregateType, e.AggregateID,
		).Scan(&current); err != nil {
			return nil, false, fmt.Errorf("postgres store: lock stream row: %w", err)
		}

		if e.ExpectedVersion != eventstore.NoVersionCheck && e.ExpectedVersion != current {
			return nil, false, eventstore.ErrVersionConflict
		}
		newVersion := current + 1

		var dedupeCol *string
		if dedupeKey != "" {
			dedupeCol = &dedupeKey
		}

		var globalSeq int64
		var createdAt time.Time
		if err := tx.QueryRow(ctx,
			`INSERT INTO events (aggregate_type, aggregate_id, version, event_type, payload, dedupe_key)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 RETURNING global_seq, created_at`,
			e.AggregateType, e.AggregateID, newVersion, e.EventType, e.Payload, dedupeCol,
		).Scan(&globalSeq, &createdAt); err != nil {
			return nil, false, fmt.Errorf("postgres store: insert event: %w", err)
		}

		if _, err := tx.Exec(ctx,
			`UPDATE stream_versions SET version = $1 WHERE aggregate_type = $2 AND aggregate_id = $3`,
			newVersion, e.AggregateType, e.AggregateID,
		); err != nil {
			return nil, false, fmt.Errorf("postgres store: bump stream version: %w", err)
		}

		if dedupeKey != "" {
			if _, err := tx.Exec(ctx,
				`INSERT INTO idempotency_key_events (dedupe_key, global_seq) VALUES ($1, $2)`,
				dedupeKey, globalSeq,
			); err != nil {
				return nil, false, fmt.Errorf("postgres store: link dedupe event: %w", err)
			}
		}

		produced = append(produced, eventstore.Event{
			GlobalSeq:     globalSeq,
			AggregateType: e.AggregateType,
			AggregateID:   e.AggregateID,
			Version:       newVersion,
			EventType:     e.EventType,
			Payload:       e.Payload,
			Metadata:      eventstore.Metadata{DedupeKey: dedupeKey},
			CreatedAt:     createdAt,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("postgres store: commit: %w", err)
	}
	return produced, false, nil
}

func (s *Store) LoadStream(ctx context.Context, aggregateType, aggregateID string) ([]eventstore.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+eventColumns+` FROM events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY version`,
		aggregateType, aggregateID,
	)
	if err != nil {
		return nil, fmt.Errorf("postgres store: load stream: %w", err)
	}
	defer rows.Close()

	var out []eventstore.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres store: scan event: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) LoadSince(ctx context.Context, afterGlobalSeq int64, limit int) ([]eventstore.Event, error) {
	query := `SELECT ` + eventColumns + ` FROM events WHERE global_seq > $1 ORDER BY global_seq`
	args := []any{afterGlobalSeq}
	if limit > 0 {
		query += ` LIMIT $2`
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres store: load since: %w", err)
	}
	defer rows.Close()

	var out []eventstore.Event
	for rows.Next() {
		ev, err := scanEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres store: scan event: %w", err)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) LatestGlobalSeq(ctx context.Context) (int64, error) {
	var seq int64
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(global_seq), 0) FROM events`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("postgres store: latest global seq: %w", err)
	}
	return seq, nil
}

func (s *Store) LookupDedupe(ctx context.Context, dedupeKey string) ([]eventstore.Event, bool, error) {
	if dedupeKey == "" {
		return nil, false, nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM idempotency_keys WHERE dedupe_key = $1)`, dedupeKey).Scan(&exists); err != nil {
		return nil, false, fmt.Errorf("postgres store: lookup dedupe: %w", err)
	}
	if !exists {
		return nil, false, nil
	}
	events, err := loadDedupeEvents(ctx, s.pool, dedupeKey)
	if err != nil {
		return nil, false, err
	}
	return events, true, nil
}

var _ eventstore.Store = (*Store)(nil)
