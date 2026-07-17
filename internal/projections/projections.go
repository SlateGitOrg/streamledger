// Package projections implements the CQRS read side: idempotent consumers
// that fold events from the log into materialized views (account balances,
// inventory levels) that the query API serves directly, without touching
// the event log or replaying history on every read.
package projections

import (
	"context"
	"time"
)

// AccountView is the materialized read model for one account.
type AccountView struct {
	AccountID    string
	BalanceCents int64
	Version      int64
	UpdatedAt    time.Time
}

// InventoryView is the materialized read model for one inventory item.
type InventoryView struct {
	ItemID            string
	AvailableQuantity int64
	ReservedQuantity  int64
	Version           int64
	UpdatedAt         time.Time
}

// ReadModelStore persists the materialized views.
//
// WithTx is what actually delivers the "no double-apply" guarantee: a
// Projector wraps its whole check-checkpoint / mutate-view / advance-checkpoint
// sequence in one WithTx call, and implementations make that a single
// atomic unit — a mutex-held critical section for the in-memory store, a
// real SQL transaction (with the checkpoint row locked FOR UPDATE) for the
// Postgres store. That's what makes it safe against a crash between
// "applied the event" and "advanced the checkpoint": either both happened,
// or neither did, so a retry/redelivery is always a clean no-op-or-redo,
// never a double-count.
type ReadModelStore interface {
	GetAccount(ctx context.Context, accountID string) (AccountView, bool, error)
	UpsertAccount(ctx context.Context, view AccountView) error

	GetInventory(ctx context.Context, itemID string) (InventoryView, bool, error)
	UpsertInventory(ctx context.Context, view InventoryView) error

	// GetCheckpoint returns the GlobalSeq of the last event this
	// projection has successfully applied (0 if none yet).
	GetCheckpoint(ctx context.Context, projectionName string) (int64, error)
	// SetCheckpoint advances the checkpoint. Implementations must ignore
	// attempts to move it backwards (SetCheckpoint is called with a
	// monotonically increasing sequence by Projector, but defending
	// against it costs nothing and removes a footgun for future callers).
	SetCheckpoint(ctx context.Context, projectionName string, globalSeq int64) error

	// WithTx runs fn with a context in which every ReadModelStore call
	// made by fn (via the same Store) participates in one atomic unit of
	// work. Implementations that are not transactional in the general
	// sense (the in-memory store) may implement this with a plain mutex;
	// what matters is that GetCheckpoint+Upsert*+SetCheckpoint inside fn
	// are indivisible from the point of view of any other caller.
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}
