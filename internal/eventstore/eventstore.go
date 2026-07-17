// Package eventstore defines the append-only event log abstraction shared by
// every StreamLedger aggregate. The log is the single source of truth: read
// models are just cached projections that can always be rebuilt from it.
//
// Two implementations satisfy this interface:
//   - memory.Store: an in-process fake used by unit tests and demos.
//   - postgres.Store: a real Postgres-backed implementation used in
//     production, exercised by an optional Testcontainers integration test.
package eventstore

import (
	"context"
	"errors"
	"time"
)

// ErrVersionConflict is returned when an Append call's ExpectedVersion does
// not match the current version of the target stream (optimistic
// concurrency failure). Callers should reload the stream and retry.
var ErrVersionConflict = errors.New("eventstore: version conflict")

// ErrStreamNotFound is returned by LoadStream when no events exist yet for
// the requested aggregate.
var ErrStreamNotFound = errors.New("eventstore: stream not found")

// NoVersionCheck disables the optimistic-concurrency check for an Append.
const NoVersionCheck int64 = -1

// StreamDoesNotExist requires that the target stream have no prior events.
const StreamDoesNotExist int64 = 0

// Metadata carries cross-cutting information about the command that caused
// an event, independent of the domain payload.
type Metadata struct {
	// DedupeKey is the idempotency key of the command that produced this
	// event (shared across every event appended by the same command call).
	DedupeKey string
	// CorrelationID ties together events that originated from the same
	// business operation (e.g. both legs of a Transfer).
	CorrelationID string
}

// AppendEvent describes one event to be appended to a stream as part of an
// Append call. ExpectedVersion is checked against the stream identified by
// (AggregateType, AggregateID) *before* any event in the batch is written;
// the whole batch is atomic.
type AppendEvent struct {
	AggregateType   string
	AggregateID     string
	EventType       string
	Payload         []byte // JSON-encoded domain payload
	ExpectedVersion int64
}

// Event is a durable, already-persisted event as read back from the log.
type Event struct {
	GlobalSeq     int64 // monotonic, store-wide ordering used for lag/catch-up
	AggregateType string
	AggregateID   string
	Version       int64 // 1-based position within its own stream
	EventType     string
	Payload       []byte
	Metadata      Metadata
	CreatedAt     time.Time
}

// Store is the append-only event log contract.
type Store interface {
	// Append persists events atomically. If dedupeKey has already been
	// successfully processed by a prior Append call, no new events are
	// written and duplicate=true is returned along with the events that
	// were produced the first time. This is what makes command handling
	// idempotent under client retries.
	//
	// If any event's ExpectedVersion does not match the current stream
	// version, ErrVersionConflict is returned and nothing is written.
	Append(ctx context.Context, dedupeKey string, events []AppendEvent) (appended []Event, duplicate bool, err error)

	// LoadStream returns every event for one aggregate, in version order.
	LoadStream(ctx context.Context, aggregateType, aggregateID string) ([]Event, error)

	// LoadSince returns up to limit events with GlobalSeq > afterGlobalSeq,
	// in global order. It is used both by the bus publisher (outbox-style
	// catch-up) and by projections doing a cold-start rebuild.
	LoadSince(ctx context.Context, afterGlobalSeq int64, limit int) ([]Event, error)

	// LatestGlobalSeq returns the current highest GlobalSeq in the store (0
	// if empty). Used to compute projection lag.
	LatestGlobalSeq(ctx context.Context) (int64, error)

	// LookupDedupe returns the events originally produced by the Append
	// call that used dedupeKey, if any. Command handlers call this before
	// evaluating business rules so that a retried command is recognized as
	// a duplicate *before* re-deciding against the (now different) current
	// state — see internal/ledger/service.go for why ordering matters here.
	LookupDedupe(ctx context.Context, dedupeKey string) (events []Event, found bool, err error)
}
