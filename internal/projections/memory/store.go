// Package memory is an in-process implementation of projections.ReadModelStore,
// used by unit tests and available as a demo-mode read side.
package memory

import (
	"context"
	"sync"

	"streamledger/internal/projections"
)

// txLockedKey is the context sentinel WithTx stamps onto the context it
// hands to fn, so that ReadModelStore calls made from inside fn know the
// store-wide mutex is already held by the current goroutine and must not
// try to acquire it again (sync.Mutex is not reentrant in Go).
type txLockedKey struct{}

func withTxLocked(ctx context.Context) context.Context {
	return context.WithValue(ctx, txLockedKey{}, true)
}

func txLocked(ctx context.Context) bool {
	v, _ := ctx.Value(txLockedKey{}).(bool)
	return v
}

// Store is a goroutine-safe in-memory projections.ReadModelStore. A single
// mutex backs every method (rather than a RWMutex) because WithTx must be
// able to hold exclusive access for its whole duration, including any
// mixture of reads and writes fn performs.
type Store struct {
	mu          sync.Mutex
	accounts    map[string]projections.AccountView
	inventory   map[string]projections.InventoryView
	checkpoints map[string]int64
}

// New constructs an empty Store.
func New() *Store {
	return &Store{
		accounts:    make(map[string]projections.AccountView),
		inventory:   make(map[string]projections.InventoryView),
		checkpoints: make(map[string]int64),
	}
}

// lock acquires the store mutex unless ctx indicates it is already held by
// the calling goroutine (i.e. we're running inside WithTx), returning the
// unlock function to defer. This lets every method be called either
// standalone or as part of a WithTx-wrapped sequence without deadlocking or
// losing atomicity.
func (s *Store) lock(ctx context.Context) func() {
	if txLocked(ctx) {
		return func() {}
	}
	s.mu.Lock()
	return s.mu.Unlock
}

func (s *Store) GetAccount(ctx context.Context, accountID string) (projections.AccountView, bool, error) {
	defer s.lock(ctx)()
	v, ok := s.accounts[accountID]
	return v, ok, nil
}

func (s *Store) UpsertAccount(ctx context.Context, view projections.AccountView) error {
	defer s.lock(ctx)()
	s.accounts[view.AccountID] = view
	return nil
}

func (s *Store) GetInventory(ctx context.Context, itemID string) (projections.InventoryView, bool, error) {
	defer s.lock(ctx)()
	v, ok := s.inventory[itemID]
	return v, ok, nil
}

func (s *Store) UpsertInventory(ctx context.Context, view projections.InventoryView) error {
	defer s.lock(ctx)()
	s.inventory[view.ItemID] = view
	return nil
}

func (s *Store) GetCheckpoint(ctx context.Context, projectionName string) (int64, error) {
	defer s.lock(ctx)()
	return s.checkpoints[projectionName], nil
}

func (s *Store) SetCheckpoint(ctx context.Context, projectionName string, globalSeq int64) error {
	defer s.lock(ctx)()
	if globalSeq > s.checkpoints[projectionName] {
		s.checkpoints[projectionName] = globalSeq
	}
	return nil
}

// WithTx holds the store's mutex for fn's entire duration, so the
// GetCheckpoint -> mutate-view -> SetCheckpoint sequence a Projector runs
// inside it is indivisible from the point of view of any concurrent
// ApplyEvent call (in-process or, once appropriately locked, from another
// goroutine racing the same event). This is what actually prevents a
// double-apply when the same event is delivered to ApplyEvent concurrently
// by two goroutines: without it, both could read the same pre-advance
// checkpoint before either writes the advanced one.
func (s *Store) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(withTxLocked(ctx))
}

var _ projections.ReadModelStore = (*Store)(nil)
