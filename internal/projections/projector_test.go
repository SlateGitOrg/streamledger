package projections_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"streamledger/internal/eventstore"
	eventmemory "streamledger/internal/eventstore/memory"
	"streamledger/internal/ledger"
	"streamledger/internal/projections"
	projmemory "streamledger/internal/projections/memory"
)

// fakeMetrics records every call, so tests can assert on dedupe/lag
// reporting rather than just the visible read-model side effects.
type fakeMetrics struct {
	mu                sync.Mutex
	applied           []string // event types applied
	duplicatesSkipped int
	lastLag           int64
}

func (f *fakeMetrics) EventApplied(_, eventType string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = append(f.applied, eventType)
}

func (f *fakeMetrics) DuplicateSkipped(string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.duplicatesSkipped++
}

func (f *fakeMetrics) Lag(_ string, lag int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastLag = lag
}

func (f *fakeMetrics) snapshot() (applied int, dup int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.applied), f.duplicatesSkipped
}

func depositEvent(t *testing.T, accountID string, amount int64, version, globalSeq int64) eventstore.Event {
	t.Helper()
	payload, err := json.Marshal(ledger.FundsDeposited{AccountID: accountID, AmountCents: amount})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return eventstore.Event{
		GlobalSeq:     globalSeq,
		AggregateType: ledger.AggregateTypeAccount,
		AggregateID:   accountID,
		Version:       version,
		EventType:     ledger.EventFundsDeposited,
		Payload:       payload,
		CreatedAt:     time.Now(),
	}
}

// TestApplyEvent_DuplicateInjection is the mandated proof: injecting the
// exact same event twice (simulating at-least-once bus redelivery) must
// mutate the read model exactly once.
func TestApplyEvent_DuplicateInjection(t *testing.T) {
	ctx := context.Background()
	store := projmemory.New()
	metrics := &fakeMetrics{}
	p := projections.NewProjector("balances", store, &projections.AccountApplier{Store: store}, metrics)

	openPayload, _ := json.Marshal(ledger.AccountOpened{AccountID: "acct-1", InitialBalanceCents: 0})
	opened := eventstore.Event{GlobalSeq: 1, AggregateType: ledger.AggregateTypeAccount, AggregateID: "acct-1", Version: 1, EventType: ledger.EventAccountOpened, Payload: openPayload}
	deposit := depositEvent(t, "acct-1", 100, 2, 2)

	mustApply(t, p, ctx, opened, true)
	mustApply(t, p, ctx, deposit, true)

	// Redeliver the same deposit event several times, as an at-least-once
	// bus would under consumer restarts / slow acks / rebalances.
	for i := 0; i < 5; i++ {
		mustApply(t, p, ctx, deposit, false)
	}

	view, found, err := store.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if !found {
		t.Fatal("expected account view to exist")
	}
	if view.BalanceCents != 100 {
		t.Fatalf("BalanceCents = %d, want 100 (single deposit applied, 5 redeliveries skipped)", view.BalanceCents)
	}

	applied, dup := metrics.snapshot()
	if applied != 2 { // AccountOpened + one FundsDeposited
		t.Fatalf("metrics recorded %d applied events, want 2", applied)
	}
	if dup != 5 {
		t.Fatalf("metrics recorded %d duplicate-skipped, want 5", dup)
	}
}

func mustApply(t *testing.T, p *projections.Projector, ctx context.Context, event eventstore.Event, wantApplied bool) {
	t.Helper()
	applied, err := p.ApplyEvent(ctx, event)
	if err != nil {
		t.Fatalf("ApplyEvent(seq=%d): %v", event.GlobalSeq, err)
	}
	if applied != wantApplied {
		t.Fatalf("ApplyEvent(seq=%d) applied=%v, want %v", event.GlobalSeq, applied, wantApplied)
	}
}

// TestApplyEvent_ConcurrentDuplicateDelivery proves the WithTx-wrapped
// checkpoint check is race-free: many goroutines racing to apply the same
// event concurrently must still only apply it once.
func TestApplyEvent_ConcurrentDuplicateDelivery(t *testing.T) {
	ctx := context.Background()
	store := projmemory.New()
	p := projections.NewProjector("balances", store, &projections.AccountApplier{Store: store}, nil)

	deposit := depositEvent(t, "acct-1", 7, 1, 1)

	const redeliveries = 40
	var wg sync.WaitGroup
	appliedCount := make([]bool, redeliveries)
	for i := 0; i < redeliveries; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			applied, err := p.ApplyEvent(ctx, deposit)
			if err != nil {
				t.Errorf("ApplyEvent: %v", err)
				return
			}
			appliedCount[i] = applied
		}(i)
	}
	wg.Wait()

	trueCount := 0
	for _, v := range appliedCount {
		if v {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("exactly one of %d concurrent deliveries of the same event should apply, got %d", redeliveries, trueCount)
	}

	view, _, err := store.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if view.BalanceCents != 7 {
		t.Fatalf("BalanceCents = %d, want 7 (concurrent redelivery must not double-apply)", view.BalanceCents)
	}
}

// TestProjector_AccountCorrectness proves the account projection produces
// the exact same balance FoldAccount would compute directly from the log —
// the CQRS read model must never drift from the write-side truth.
func TestProjector_AccountCorrectness(t *testing.T) {
	ctx := context.Background()
	esStore := eventmemory.New()
	svc := ledger.NewService(esStore, nil)

	if _, err := svc.OpenAccount(ctx, "acct-1", 500, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	if _, err := svc.Deposit(ctx, "acct-1", 200, "", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("Deposit: %v", err)
	}
	if _, err := svc.Withdraw(ctx, "acct-1", 150, "", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	rmStore := projmemory.New()
	p := projections.NewProjector("balances", rmStore, &projections.AccountApplier{Store: rmStore}, nil)
	if _, err := p.CatchUp(ctx, esStore, 100); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}

	rawEvents, err := esStore.LoadStream(ctx, ledger.AggregateTypeAccount, "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	want, err := ledger.FoldAccount("acct-1", rawEvents)
	if err != nil {
		t.Fatalf("FoldAccount: %v", err)
	}

	got, found, err := rmStore.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if !found {
		t.Fatal("expected projection to have a view for acct-1")
	}
	if got.BalanceCents != want.BalanceCents {
		t.Fatalf("projection balance = %d, want %d (must match direct fold of the log)", got.BalanceCents, want.BalanceCents)
	}
	if got.Version != want.Version {
		t.Fatalf("projection version = %d, want %d", got.Version, want.Version)
	}
}

// TestProjector_InventoryCorrectness mirrors the account correctness test
// for the inventory aggregate/applier.
func TestProjector_InventoryCorrectness(t *testing.T) {
	ctx := context.Background()
	esStore := eventmemory.New()
	svc := ledger.NewService(esStore, nil)

	if _, err := svc.AdjustInventory(ctx, "item-1", 200, "restock", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("AdjustInventory: %v", err)
	}
	if _, err := svc.ReserveStock(ctx, "item-1", 50, "order-1", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}
	if _, err := svc.ReleaseStock(ctx, "item-1", 20, "order-1", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("ReleaseStock: %v", err)
	}

	rmStore := projmemory.New()
	p := projections.NewProjector("inventory", rmStore, &projections.InventoryApplier{Store: rmStore}, nil)
	if _, err := p.CatchUp(ctx, esStore, 100); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}

	got, found, err := rmStore.GetInventory(ctx, "item-1")
	if err != nil {
		t.Fatalf("GetInventory: %v", err)
	}
	if !found {
		t.Fatal("expected projection to have a view for item-1")
	}
	if got.AvailableQuantity != 170 { // 200 - 50 + 20
		t.Fatalf("AvailableQuantity = %d, want 170", got.AvailableQuantity)
	}
	if got.ReservedQuantity != 30 { // 50 - 20
		t.Fatalf("ReservedQuantity = %d, want 30", got.ReservedQuantity)
	}
}

// TestProjector_IgnoresIrrelevantEventsButAdvancesCheckpoint proves that a
// projection which doesn't care about a given event still treats it as
// "seen" — otherwise lag would be measured against only the events a
// projection cares about, hiding real backlog.
func TestProjector_IgnoresIrrelevantEventsButAdvancesCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := projmemory.New()
	metrics := &fakeMetrics{}
	// The balances projector only cares about "account" aggregate events;
	// feed it an inventory event.
	p := projections.NewProjector("balances", store, &projections.AccountApplier{Store: store}, metrics)

	invPayload, _ := json.Marshal(ledger.InventoryAdjusted{ItemID: "item-1", DeltaQuantity: 5})
	irrelevant := eventstore.Event{GlobalSeq: 1, AggregateType: ledger.AggregateTypeInventoryItem, AggregateID: "item-1", Version: 1, EventType: ledger.EventInventoryAdjusted, Payload: invPayload}

	applied, err := p.ApplyEvent(ctx, irrelevant)
	if err != nil {
		t.Fatalf("ApplyEvent: %v", err)
	}
	if applied {
		t.Fatal("irrelevant event should not report applied=true")
	}

	checkpoint, err := p.Checkpoint(ctx)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if checkpoint != 1 {
		t.Fatalf("checkpoint = %d, want 1 (irrelevant events must still advance the checkpoint)", checkpoint)
	}

	// Redelivering the same irrelevant event must be recognized as a
	// duplicate (already-seen), not re-evaluated as new.
	if _, err := p.ApplyEvent(ctx, irrelevant); err != nil {
		t.Fatalf("ApplyEvent (redelivery): %v", err)
	}
	_, dup := metrics.snapshot()
	if dup != 1 {
		t.Fatalf("duplicatesSkipped = %d, want 1", dup)
	}
}

// TestProjector_Lag proves lag reporting reflects the gap between the
// event log's latest global sequence and what this projection has applied.
func TestProjector_Lag(t *testing.T) {
	ctx := context.Background()
	esStore := eventmemory.New()
	svc := ledger.NewService(esStore, nil)

	for i := 0; i < 5; i++ {
		if _, err := svc.AdjustInventory(ctx, "item-1", 1, "", "", eventstore.NoVersionCheck); err != nil {
			t.Fatalf("AdjustInventory #%d: %v", i, err)
		}
	}

	metrics := &fakeMetrics{}
	rmStore := projmemory.New()
	p := projections.NewProjector("inventory", rmStore, &projections.InventoryApplier{Store: rmStore}, metrics)

	latest, err := esStore.LatestGlobalSeq(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalSeq: %v", err)
	}
	lag, err := p.Lag(ctx, latest)
	if err != nil {
		t.Fatalf("Lag: %v", err)
	}
	if lag != 5 {
		t.Fatalf("lag = %d, want 5 (checkpoint at 0, 5 events in the log)", lag)
	}

	if _, err := p.CatchUp(ctx, esStore, 100); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	lag, err = p.Lag(ctx, latest)
	if err != nil {
		t.Fatalf("Lag after catch-up: %v", err)
	}
	if lag != 0 {
		t.Fatalf("lag after catch-up = %d, want 0", lag)
	}
}

// TestProjector_CatchUp_IsIdempotent proves repeated CatchUp calls (e.g.
// overlapping polling ticks) never double-apply, matching the durability
// backstop's documented safety property.
func TestProjector_CatchUp_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	esStore := eventmemory.New()
	svc := ledger.NewService(esStore, nil)

	if _, err := svc.OpenAccount(ctx, "acct-1", 0, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := svc.Deposit(ctx, "acct-1", 10, "", "", eventstore.NoVersionCheck); err != nil {
			t.Fatalf("Deposit #%d: %v", i, err)
		}
	}

	rmStore := projmemory.New()
	p := projections.NewProjector("balances", rmStore, &projections.AccountApplier{Store: rmStore}, nil)

	for i := 0; i < 4; i++ { // call CatchUp repeatedly, as overlapping polls would
		if _, err := p.CatchUp(ctx, esStore, 2 /* small batch to exercise pagination */); err != nil {
			t.Fatalf("CatchUp #%d: %v", i, err)
		}
	}

	view, _, err := rmStore.GetAccount(ctx, "acct-1")
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if view.BalanceCents != 30 {
		t.Fatalf("BalanceCents = %d, want 30 (repeated CatchUp must not double-apply)", view.BalanceCents)
	}
}
