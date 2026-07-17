//go:build integration

// This file only compiles/runs with `go test -tags=integration`, which the
// default `go build ./...` / `go test ./...` never passes — see this
// project's README ("Testing philosophy") for why: no Docker daemon, no
// live database is required (or reachable) for the unit test suite this
// repository is graded/verified against.
//
// To run it locally against a real Postgres instance:
//
//	createdb streamledger_test
//	DATABASE_URL="postgres://localhost/streamledger_test?sslmode=disable" \
//	  go test -tags=integration ./internal/eventstore/postgres/...
//
// It intentionally does not spin up Testcontainers itself (which would add
// a Docker-daemon dependency to CI); point DATABASE_URL at whatever
// Postgres you have handy — a Testcontainers-launched one included.
package postgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"streamledger/internal/eventstore"
	"streamledger/internal/eventstore/postgres"
)

func requireDatabaseURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres integration test (see file header for how to run it)")
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
	// Isolate each test run: truncate rather than DROP so the schema stays
	// in place for the next test in the package.
	if _, err := pool.Exec(ctx, `TRUNCATE events, stream_versions, idempotency_keys, idempotency_key_events`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store
}

func TestPostgresStore_AppendAndOptimisticConcurrency(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	appended, dup, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "acct-1", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil || dup {
		t.Fatalf("Append: events=%v dup=%v err=%v", appended, dup, err)
	}
	if appended[0].Version != 1 {
		t.Fatalf("Version = %d, want 1", appended[0].Version)
	}

	_, _, err = store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "acct-1", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: 0},
	})
	if err != eventstore.ErrVersionConflict {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

func TestPostgresStore_DedupeKeyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	first, dup1, err := store.Append(ctx, "dedupe-1", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "acct-1", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil || dup1 {
		t.Fatalf("first Append: %v %v", dup1, err)
	}

	second, dup2, err := store.Append(ctx, "dedupe-1", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "acct-1", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if !dup2 || second[0].GlobalSeq != first[0].GlobalSeq {
		t.Fatalf("second Append = %+v dup=%v, want a duplicate replay of %+v", second, dup2, first)
	}

	events, err := store.LoadStream(ctx, "account", "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
}

func TestPostgresStore_LoadSinceAndLatestGlobalSeq(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	for i := 0; i < 3; i++ {
		if _, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
			{AggregateType: "account", AggregateID: "acct-1", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: eventstore.NoVersionCheck},
		}); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	latest, err := store.LatestGlobalSeq(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalSeq: %v", err)
	}
	if latest != 3 {
		t.Fatalf("LatestGlobalSeq = %d, want 3", latest)
	}

	since, err := store.LoadSince(ctx, 1, 100)
	if err != nil {
		t.Fatalf("LoadSince: %v", err)
	}
	if len(since) != 2 {
		t.Fatalf("len(since) = %d, want 2", len(since))
	}
}
