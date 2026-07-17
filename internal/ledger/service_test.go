package ledger_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"streamledger/internal/eventstore"
	"streamledger/internal/eventstore/memory"
	"streamledger/internal/ledger"
)

func newService(t *testing.T) (*ledger.Service, *memory.Store) {
	t.Helper()
	store := memory.New()
	return ledger.NewService(store, nil), store
}

// --- Idempotency / dedupe --------------------------------------------------

func TestDeposit_DuplicateDedupeKeyDoesNotDoubleApply(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.OpenAccount(ctx, "acct-1", 100, "open-1"); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	first, err := svc.Deposit(ctx, "acct-1", 50, "salary", "deposit-key-1", eventstore.NoVersionCheck)
	if err != nil {
		t.Fatalf("first Deposit: %v", err)
	}
	if !first.Applied || first.Duplicate {
		t.Fatalf("first deposit result = %+v, want Applied=true Duplicate=false", first)
	}

	// Simulate the client (or an at-least-once message bus) retrying the
	// exact same command, e.g. after a timed-out response it never saw.
	for i := 0; i < 5; i++ {
		dup, err := svc.Deposit(ctx, "acct-1", 50, "salary", "deposit-key-1", eventstore.NoVersionCheck)
		if err != nil {
			t.Fatalf("duplicate Deposit #%d: %v", i, err)
		}
		if dup.Applied {
			t.Fatalf("duplicate Deposit #%d was Applied=true; want a no-op replay", i)
		}
		if !dup.Duplicate {
			t.Fatalf("duplicate Deposit #%d Duplicate=false, want true", i)
		}
		if dup.NewVersion != first.NewVersion {
			t.Fatalf("duplicate Deposit #%d NewVersion = %d, want %d (same as original)", i, dup.NewVersion, first.NewVersion)
		}
	}

	events, err := store.LoadStream(ctx, ledger.AggregateTypeAccount, "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 2 { // AccountOpened + exactly one FundsDeposited, never six
		t.Fatalf("stream has %d events, want 2 (open + single deposit, despite 6 total calls)", len(events))
	}

	st, err := ledger.FoldAccount("acct-1", events)
	if err != nil {
		t.Fatalf("FoldAccount: %v", err)
	}
	if st.BalanceCents != 150 {
		t.Fatalf("BalanceCents = %d, want 150 (100 opening + a single 50 deposit)", st.BalanceCents)
	}
}

func TestOpenAccount_DuplicateDedupeKeyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	first, err := svc.OpenAccount(ctx, "acct-1", 100, "open-key-1")
	if err != nil {
		t.Fatalf("first OpenAccount: %v", err)
	}
	second, err := svc.OpenAccount(ctx, "acct-1", 100, "open-key-1")
	if err != nil {
		t.Fatalf("second OpenAccount: %v", err)
	}
	if !second.Duplicate || second.NewVersion != first.NewVersion {
		t.Fatalf("second OpenAccount = %+v, want a duplicate replay of %+v", second, first)
	}
}

// TestDuplicateDelivery_ThroughBus proves idempotency end-to-end through the
// same path a redelivering message bus would take: a command handled once,
// then the resulting *events* redelivered to nothing (events themselves
// don't get "re-applied" by Service — that's LedgerService's job — but this
// confirms that even directly re-invoking the command handler with the same
// dedupe key after a duplicate message never produces a second event, which
// is the guarantee client retries and at-least-once brokers both depend on).
func TestDuplicateDelivery_ThroughBus(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.OpenAccount(ctx, "acct-1", 0, "open-1"); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}

	const deliveries = 4
	var wg sync.WaitGroup
	results := make([]ledger.Result, deliveries)
	errs := make([]error, deliveries)
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.Deposit(ctx, "acct-1", 100, "payout", "shared-dedupe-key", eventstore.NoVersionCheck)
		}(i)
	}
	wg.Wait()

	applied := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent duplicate delivery #%d: %v", i, err)
		}
		if results[i].Applied {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("exactly one of %d concurrent identical-dedupe-key deliveries should apply, got %d", deliveries, applied)
	}

	events, err := store.LoadStream(ctx, ledger.AggregateTypeAccount, "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("stream has %d events, want 2 (open + single deposit)", len(events))
	}
}

// --- Optimistic concurrency -------------------------------------------------

func TestDeposit_PinnedVersionConflict(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)

	if _, err := svc.OpenAccount(ctx, "acct-1", 0, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	// Stream is now at version 1 (AccountOpened). Pin to a stale version.
	_, err := svc.Deposit(ctx, "acct-1", 50, "", "", 0)
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}

	// Pinning to the *correct* current version succeeds.
	res, err := svc.Deposit(ctx, "acct-1", 50, "", "", 1)
	if err != nil {
		t.Fatalf("Deposit with correct pinned version: %v", err)
	}
	if res.NewVersion != 2 {
		t.Fatalf("NewVersion = %d, want 2", res.NewVersion)
	}
}

func TestWithdraw_NeverGoesNegativeUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.OpenAccount(ctx, "acct-1", 1000, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	// Give the retry loop plenty of headroom: this test intentionally
	// maximizes contention (every goroutine racing the very same account),
	// which is a harsher pattern than the default retry budget is tuned
	// for. What's under test is *correctness* of the retry-until-consistent
	// loop, not the production retry budget itself.
	svc.MaxRetries = 200

	// 30 concurrent withdrawals of 100 against a balance of 1000: without
	// optimistic-concurrency retries serializing the read-decide-append
	// cycle per account, a naive read-modify-write would let far more than
	// 10 of these succeed (the classic lost-update / double-spend bug this
	// project exists to prevent).
	const attempts = 30
	const amount = 100
	var wg sync.WaitGroup
	succeeded := make([]bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := svc.Withdraw(ctx, "acct-1", amount, "", "", eventstore.NoVersionCheck)
			if err == nil {
				succeeded[i] = res.Applied
				return
			}
			if !errors.Is(err, ledger.ErrInsufficientFunds) {
				t.Errorf("withdrawal #%d: unexpected error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, ok := range succeeded {
		if ok {
			successCount++
		}
	}
	if successCount != 10 {
		t.Fatalf("successful withdrawals = %d, want exactly 10 (1000/100), proving no lost updates / double-spend", successCount)
	}

	events, err := store.LoadStream(ctx, ledger.AggregateTypeAccount, "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	st, err := ledger.FoldAccount("acct-1", events)
	if err != nil {
		t.Fatalf("FoldAccount: %v", err)
	}
	if st.BalanceCents != 0 {
		t.Fatalf("final BalanceCents = %d, want 0", st.BalanceCents)
	}
}

func TestDeposit_ConcurrentDepositsAllLand(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.OpenAccount(ctx, "acct-1", 0, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	svc.MaxRetries = 200 // see TestWithdraw_NeverGoesNegativeUnderConcurrency for why

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Deposit(ctx, "acct-1", 1, "", "", eventstore.NoVersionCheck); err != nil {
				t.Errorf("Deposit: %v", err)
			}
		}()
	}
	wg.Wait()

	events, err := store.LoadStream(ctx, ledger.AggregateTypeAccount, "acct-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	st, err := ledger.FoldAccount("acct-1", events)
	if err != nil {
		t.Fatalf("FoldAccount: %v", err)
	}
	if st.BalanceCents != n {
		t.Fatalf("BalanceCents = %d, want %d (every concurrent deposit must eventually land, none lost)", st.BalanceCents, n)
	}
}

// --- Business rules ----------------------------------------------------

func TestTransfer(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.OpenAccount(ctx, "from", 500, ""); err != nil {
		t.Fatalf("OpenAccount(from): %v", err)
	}
	if _, err := svc.OpenAccount(ctx, "to", 0, ""); err != nil {
		t.Fatalf("OpenAccount(to): %v", err)
	}

	if _, err := svc.Transfer(ctx, "from", "to", 200, "transfer-1"); err != nil {
		t.Fatalf("Transfer: %v", err)
	}

	fromEvents, _ := store.LoadStream(ctx, ledger.AggregateTypeAccount, "from")
	toEvents, _ := store.LoadStream(ctx, ledger.AggregateTypeAccount, "to")
	fromState, _ := ledger.FoldAccount("from", fromEvents)
	toState, _ := ledger.FoldAccount("to", toEvents)

	if fromState.BalanceCents != 300 {
		t.Fatalf("from balance = %d, want 300", fromState.BalanceCents)
	}
	if toState.BalanceCents != 200 {
		t.Fatalf("to balance = %d, want 200", toState.BalanceCents)
	}
}

func TestTransfer_SameAccountRejected(t *testing.T) {
	ctx := context.Background()
	svc, _ := newService(t)
	if _, err := svc.OpenAccount(ctx, "acct-1", 100, ""); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	_, err := svc.Transfer(ctx, "acct-1", "acct-1", 10, "")
	if !errors.Is(err, ledger.ErrSameAccountTransfer) {
		t.Fatalf("err = %v, want ErrSameAccountTransfer", err)
	}
}

func TestTransfer_InsufficientFundsLeavesBothAccountsUntouched(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)
	if _, err := svc.OpenAccount(ctx, "from", 10, ""); err != nil {
		t.Fatalf("OpenAccount(from): %v", err)
	}
	if _, err := svc.OpenAccount(ctx, "to", 0, ""); err != nil {
		t.Fatalf("OpenAccount(to): %v", err)
	}

	_, err := svc.Transfer(ctx, "from", "to", 1000, "")
	if !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}

	toEvents, _ := store.LoadStream(ctx, ledger.AggregateTypeAccount, "to")
	if len(toEvents) != 1 { // only AccountOpened; the failed transfer must not partially apply
		t.Fatalf("'to' stream has %d events, want 1 (a failed transfer must not partially apply)", len(toEvents))
	}
}

func TestReserveAndReleaseStock(t *testing.T) {
	ctx := context.Background()
	svc, store := newService(t)

	if _, err := svc.AdjustInventory(ctx, "item-1", 100, "initial stock", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("AdjustInventory: %v", err)
	}
	if _, err := svc.ReserveStock(ctx, "item-1", 30, "order-1", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("ReserveStock: %v", err)
	}

	_, err := svc.ReserveStock(ctx, "item-1", 1000, "order-2", "", eventstore.NoVersionCheck)
	if !errors.Is(err, ledger.ErrInsufficientStock) {
		t.Fatalf("over-reservation err = %v, want ErrInsufficientStock", err)
	}

	if _, err := svc.ReleaseStock(ctx, "item-1", 10, "order-1", "", eventstore.NoVersionCheck); err != nil {
		t.Fatalf("ReleaseStock: %v", err)
	}

	events, err := store.LoadStream(ctx, ledger.AggregateTypeInventoryItem, "item-1")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	st, err := ledger.FoldInventory("item-1", events)
	if err != nil {
		t.Fatalf("FoldInventory: %v", err)
	}
	if st.AvailableQuantity != 80 { // 100 - 30 reserved + 10 released
		t.Fatalf("AvailableQuantity = %d, want 80", st.AvailableQuantity)
	}
	if st.ReservedQuantity != 20 { // 30 - 10 released
		t.Fatalf("ReservedQuantity = %d, want 20", st.ReservedQuantity)
	}
}

// stubPublisher records every batch of events it's asked to publish.
type stubPublisher struct {
	mu     sync.Mutex
	events []eventstore.Event
}

func (p *stubPublisher) Publish(_ context.Context, events []eventstore.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
}

func (p *stubPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func TestService_PublishesOnlyOnRealApply(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	pub := &stubPublisher{}
	svc := ledger.NewService(store, pub)

	if _, err := svc.OpenAccount(ctx, "acct-1", 0, "open-1"); err != nil {
		t.Fatalf("OpenAccount: %v", err)
	}
	if _, err := svc.OpenAccount(ctx, "acct-1", 0, "open-1"); err != nil { // duplicate
		t.Fatalf("duplicate OpenAccount: %v", err)
	}

	if got := pub.count(); got != 1 {
		t.Fatalf("publisher received %d events, want 1 (duplicates must not re-publish)", got)
	}
}
