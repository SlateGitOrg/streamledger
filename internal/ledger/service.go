package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"streamledger/internal/eventstore"
)

// defaultMaxRetries bounds how many times a command re-decides and retries
// its append after losing an optimistic-concurrency race. Under realistic
// contention (a handful of concurrent writers per hot account) a handful of
// retries is more than enough; it exists to convert transient conflicts
// into success instead of pushing every collision back to the client.
const defaultMaxRetries = 8

// Result is the outcome of a command, mirroring the gRPC CommandResponse.
type Result struct {
	// Applied is true if this call produced new events.
	Applied bool
	// Duplicate is true if dedupeKey had already been processed; Result
	// reflects the cached outcome of the original call, not a new mutation.
	Duplicate bool
	// NewVersion is the resulting version of the primary aggregate stream
	// (the account being deposited/withdrawn from, the inventory item
	// being adjusted, etc).
	NewVersion int64
}

// EventPublisher is notified, best-effort, whenever the Service appends new
// events. It must not block for long and must not be relied upon for
// correctness: the event log is the durable source of truth, and any
// consumer must be able to catch up by polling eventstore.Store.LoadSince
// regardless of whether Publish was ever called or delivered. A nil
// EventPublisher is valid and simply disables the fast-path notification.
type EventPublisher interface {
	Publish(ctx context.Context, events []eventstore.Event)
}

// Service is the command-handling layer: it loads current aggregate state,
// applies the pure Decide* business rules, and appends the resulting
// event(s) with an optimistic-concurrency check, retrying automatically on
// losing a concurrent race.
type Service struct {
	Store      eventstore.Store
	Publisher  EventPublisher // optional
	MaxRetries int
}

// NewService constructs a Service backed by store. publisher may be nil.
func NewService(store eventstore.Store, publisher EventPublisher) *Service {
	return &Service{Store: store, Publisher: publisher, MaxRetries: defaultMaxRetries}
}

func (s *Service) maxRetries() int {
	if s.MaxRetries <= 0 {
		return defaultMaxRetries
	}
	return s.MaxRetries
}

func (s *Service) publish(ctx context.Context, events []eventstore.Event) {
	if s.Publisher != nil && len(events) > 0 {
		s.Publisher.Publish(ctx, events)
	}
}

func (s *Service) loadAccount(ctx context.Context, accountID string) (AccountState, error) {
	events, err := s.Store.LoadStream(ctx, AggregateTypeAccount, accountID)
	if err != nil {
		return AccountState{}, err
	}
	return FoldAccount(accountID, events)
}

func (s *Service) loadInventory(ctx context.Context, itemID string) (InventoryState, error) {
	events, err := s.Store.LoadStream(ctx, AggregateTypeInventoryItem, itemID)
	if err != nil {
		return InventoryState{}, err
	}
	return FoldInventory(itemID, events)
}

// duplicateResult builds a Result from events previously produced by an
// identical dedupeKey, locating the version of the given (aggregateType,
// aggregateID) stream within them.
func duplicateResult(events []eventstore.Event, aggregateType, aggregateID string) Result {
	res := Result{Duplicate: true, Applied: false}
	for _, e := range events {
		if e.AggregateType == aggregateType && e.AggregateID == aggregateID {
			res.NewVersion = e.Version
		}
	}
	return res
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// All payload types here are plain structs of primitives; a
		// marshal failure would be a programmer error, not a runtime one.
		panic(fmt.Sprintf("ledger: failed to marshal event payload: %v", err))
	}
	return b
}

// --- Account commands -------------------------------------------------

// OpenAccount creates a new account stream with an initial balance.
func (s *Service) OpenAccount(ctx context.Context, accountID string, initialBalanceCents int64, dedupeKey string) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeAccount, accountID), nil
		}
	}

	current, err := s.loadAccount(ctx, accountID)
	if err != nil {
		return Result{}, err
	}
	event, err := DecideOpenAccount(current, accountID, initialBalanceCents)
	if err != nil {
		return Result{}, err
	}
	appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
		AggregateType:   AggregateTypeAccount,
		AggregateID:     accountID,
		EventType:       EventAccountOpened,
		Payload:         mustMarshal(event),
		ExpectedVersion: eventstore.StreamDoesNotExist,
	}})
	if err != nil {
		return Result{}, err
	}
	if dup {
		return duplicateResult(appended, AggregateTypeAccount, accountID), nil
	}
	s.publish(ctx, appended)
	return Result{Applied: true, NewVersion: appended[0].Version}, nil
}

// versionPlan resolves how many attempts a command gets and which
// ExpectedVersion to send with each Append, given the caller's requested
// expectedVersion:
//
//   - eventstore.NoVersionCheck (-1): "auto" mode. The handler loads the
//     stream's current version fresh on every attempt and retries up to
//     maxRetries times on ErrVersionConflict — the normal path for a
//     server-decided write with no client-visible version pinning.
//   - any N >= 0: "pinned" mode, a single attempt using exactly N as the
//     ExpectedVersion. This is a compare-and-swap: if the stream has moved
//     on, the caller gets ErrVersionConflict back immediately and decides
//     whether to reload and retry (typical optimistic-locking UI pattern).
type versionPlan struct {
	pinned      bool
	pinnedValue int64
	maxAttempts int
}

func (s *Service) planVersion(expectedVersion int64) versionPlan {
	if expectedVersion == eventstore.NoVersionCheck {
		return versionPlan{maxAttempts: s.maxRetries() + 1}
	}
	return versionPlan{pinned: true, pinnedValue: expectedVersion, maxAttempts: 1}
}

func (p versionPlan) expectedVersionFor(current int64) int64 {
	if p.pinned {
		return p.pinnedValue
	}
	return current
}

// Deposit credits an account. If expectedVersion is eventstore.NoVersionCheck
// the handler retries automatically on optimistic-concurrency conflicts
// from concurrent writers on the same account; otherwise it performs a
// single compare-and-swap attempt against the caller-supplied version.
func (s *Service) Deposit(ctx context.Context, accountID string, amountCents int64, reason, dedupeKey string, expectedVersion int64) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeAccount, accountID), nil
		}
	}

	plan := s.planVersion(expectedVersion)
	for attempt := 0; attempt < plan.maxAttempts; attempt++ {
		current, err := s.loadAccount(ctx, accountID)
		if err != nil {
			return Result{}, err
		}
		event, err := DecideDeposit(current, amountCents, reason)
		if err != nil {
			return Result{}, err
		}
		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
			AggregateType:   AggregateTypeAccount,
			AggregateID:     accountID,
			EventType:       EventFundsDeposited,
			Payload:         mustMarshal(event),
			ExpectedVersion: plan.expectedVersionFor(current.Version),
		}})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < plan.maxAttempts-1 {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeAccount, accountID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
	return Result{}, eventstore.ErrVersionConflict
}

// Withdraw debits an account, enforcing that the balance never goes
// negative even under concurrent withdrawals against the same account. See
// Deposit's doc comment for expectedVersion semantics.
func (s *Service) Withdraw(ctx context.Context, accountID string, amountCents int64, reason, dedupeKey string, expectedVersion int64) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeAccount, accountID), nil
		}
	}

	plan := s.planVersion(expectedVersion)
	for attempt := 0; attempt < plan.maxAttempts; attempt++ {
		current, err := s.loadAccount(ctx, accountID)
		if err != nil {
			return Result{}, err
		}
		event, err := DecideWithdraw(current, amountCents, reason)
		if err != nil {
			return Result{}, err
		}
		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
			AggregateType:   AggregateTypeAccount,
			AggregateID:     accountID,
			EventType:       EventFundsWithdrawn,
			Payload:         mustMarshal(event),
			ExpectedVersion: plan.expectedVersionFor(current.Version),
		}})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < plan.maxAttempts-1 {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeAccount, accountID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
	return Result{}, eventstore.ErrVersionConflict
}

// Transfer debits fromAccountID and credits toAccountID atomically: both
// events are appended in a single Store.Append call, so a Postgres-backed
// Store commits them in one transaction (either both land or neither does),
// with the source's insufficient-funds check re-evaluated on every retry.
func (s *Service) Transfer(ctx context.Context, fromAccountID, toAccountID string, amountCents int64, dedupeKey string) (Result, error) {
	if fromAccountID == toAccountID {
		return Result{}, ErrSameAccountTransfer
	}
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeAccount, fromAccountID), nil
		}
	}

	reason := "transfer:" + dedupeKey
	for attempt := 0; ; attempt++ {
		from, err := s.loadAccount(ctx, fromAccountID)
		if err != nil {
			return Result{}, err
		}
		to, err := s.loadAccount(ctx, toAccountID)
		if err != nil {
			return Result{}, err
		}
		withdrawEvent, err := DecideWithdraw(from, amountCents, reason)
		if err != nil {
			return Result{}, err
		}
		depositEvent, err := DecideDeposit(to, amountCents, reason)
		if err != nil {
			return Result{}, err
		}

		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{
			{
				AggregateType:   AggregateTypeAccount,
				AggregateID:     fromAccountID,
				EventType:       EventFundsWithdrawn,
				Payload:         mustMarshal(withdrawEvent),
				ExpectedVersion: from.Version,
			},
			{
				AggregateType:   AggregateTypeAccount,
				AggregateID:     toAccountID,
				EventType:       EventFundsDeposited,
				Payload:         mustMarshal(depositEvent),
				ExpectedVersion: to.Version,
			},
		})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < s.maxRetries() {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeAccount, fromAccountID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
}

// --- Inventory commands -------------------------------------------------

// AdjustInventory applies a signed stock-level change (restock, shrinkage,
// manual correction). See Deposit's doc comment for expectedVersion
// semantics.
func (s *Service) AdjustInventory(ctx context.Context, itemID string, delta int64, reason, dedupeKey string, expectedVersion int64) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeInventoryItem, itemID), nil
		}
	}

	plan := s.planVersion(expectedVersion)
	for attempt := 0; attempt < plan.maxAttempts; attempt++ {
		current, err := s.loadInventory(ctx, itemID)
		if err != nil {
			return Result{}, err
		}
		event, err := DecideAdjustInventory(current, itemID, delta, reason)
		if err != nil {
			return Result{}, err
		}
		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
			AggregateType:   AggregateTypeInventoryItem,
			AggregateID:     itemID,
			EventType:       EventInventoryAdjusted,
			Payload:         mustMarshal(event),
			ExpectedVersion: plan.expectedVersionFor(current.Version),
		}})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < plan.maxAttempts-1 {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeInventoryItem, itemID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
	return Result{}, eventstore.ErrVersionConflict
}

// ReserveStock moves quantity from available to reserved for an order. See
// Deposit's doc comment for expectedVersion semantics.
func (s *Service) ReserveStock(ctx context.Context, itemID string, quantity int64, orderID, dedupeKey string, expectedVersion int64) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeInventoryItem, itemID), nil
		}
	}

	plan := s.planVersion(expectedVersion)
	for attempt := 0; attempt < plan.maxAttempts; attempt++ {
		current, err := s.loadInventory(ctx, itemID)
		if err != nil {
			return Result{}, err
		}
		event, err := DecideReserveStock(current, itemID, quantity, orderID)
		if err != nil {
			return Result{}, err
		}
		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
			AggregateType:   AggregateTypeInventoryItem,
			AggregateID:     itemID,
			EventType:       EventStockReserved,
			Payload:         mustMarshal(event),
			ExpectedVersion: plan.expectedVersionFor(current.Version),
		}})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < plan.maxAttempts-1 {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeInventoryItem, itemID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
	return Result{}, eventstore.ErrVersionConflict
}

// ReleaseStock moves quantity from reserved back to available. See
// Deposit's doc comment for expectedVersion semantics.
func (s *Service) ReleaseStock(ctx context.Context, itemID string, quantity int64, orderID, dedupeKey string, expectedVersion int64) (Result, error) {
	if dedupeKey != "" {
		if events, found, err := s.Store.LookupDedupe(ctx, dedupeKey); err != nil {
			return Result{}, err
		} else if found {
			return duplicateResult(events, AggregateTypeInventoryItem, itemID), nil
		}
	}

	plan := s.planVersion(expectedVersion)
	for attempt := 0; attempt < plan.maxAttempts; attempt++ {
		current, err := s.loadInventory(ctx, itemID)
		if err != nil {
			return Result{}, err
		}
		event, err := DecideReleaseStock(current, itemID, quantity, orderID)
		if err != nil {
			return Result{}, err
		}
		appended, dup, err := s.Store.Append(ctx, dedupeKey, []eventstore.AppendEvent{{
			AggregateType:   AggregateTypeInventoryItem,
			AggregateID:     itemID,
			EventType:       EventStockReleased,
			Payload:         mustMarshal(event),
			ExpectedVersion: plan.expectedVersionFor(current.Version),
		}})
		if errors.Is(err, eventstore.ErrVersionConflict) {
			if attempt < plan.maxAttempts-1 {
				continue
			}
			return Result{}, err
		}
		if err != nil {
			return Result{}, err
		}
		if dup {
			return duplicateResult(appended, AggregateTypeInventoryItem, itemID), nil
		}
		s.publish(ctx, appended)
		return Result{Applied: true, NewVersion: appended[0].Version}, nil
	}
	return Result{}, eventstore.ErrVersionConflict
}
