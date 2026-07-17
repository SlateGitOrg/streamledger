package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"streamledger/internal/eventstore"
	"streamledger/internal/eventstore/memory"
)

func TestAppend_AssignsMonotonicVersionsAndGlobalSeq(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	ev1, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if ev1[0].Version != 1 || ev1[0].GlobalSeq != 1 {
		t.Fatalf("first event = %+v, want Version=1 GlobalSeq=1", ev1[0])
	}

	ev2, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: 1},
	})
	if err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if ev2[0].Version != 2 || ev2[0].GlobalSeq != 2 {
		t.Fatalf("second event = %+v, want Version=2 GlobalSeq=2", ev2[0])
	}

	// A second, unrelated stream gets its own version numbering but shares
	// the store-wide GlobalSeq sequence.
	ev3, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "b", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil {
		t.Fatalf("third Append: %v", err)
	}
	if ev3[0].Version != 1 || ev3[0].GlobalSeq != 3 {
		t.Fatalf("third event = %+v, want Version=1 GlobalSeq=3", ev3[0])
	}
}

func TestAppend_OptimisticConcurrencyConflict(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if _, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Stream is at version 1. Expecting 0 (does-not-exist) now conflicts.
	_, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}

	// Expecting a stale version also conflicts.
	_, _, err = store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: 99},
	})
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
}

func TestAppend_NoVersionCheckAlwaysSucceeds(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	for i := 0; i < 5; i++ {
		if _, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
			{AggregateType: "account", AggregateID: "a", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: eventstore.NoVersionCheck},
		}); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	events, err := store.LoadStream(ctx, "account", "a")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("len(events) = %d, want 5", len(events))
	}
}

func TestAppend_BatchIsAllOrNothing(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if _, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "from", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	}); err != nil {
		t.Fatalf("seed from: %v", err)
	}

	// "to" does not exist, so ExpectedVersion: 5 must conflict, and the
	// whole batch (including the valid "from" event) must be rejected.
	_, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "from", EventType: "Debited", Payload: []byte(`{}`), ExpectedVersion: 1},
		{AggregateType: "account", AggregateID: "to", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: 5},
	})
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}

	fromEvents, err := store.LoadStream(ctx, "account", "from")
	if err != nil {
		t.Fatalf("LoadStream(from): %v", err)
	}
	if len(fromEvents) != 1 {
		t.Fatalf("'from' has %d events, want 1 (the failed batch must not partially apply)", len(fromEvents))
	}
}

func TestAppend_DedupeKeyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	first, dup1, err := store.Append(ctx, "key-1", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{"n":1}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil || dup1 {
		t.Fatalf("first Append: events=%v dup=%v err=%v", first, dup1, err)
	}

	second, dup2, err := store.Append(ctx, "key-1", []eventstore.AppendEvent{
		// Deliberately a *different* payload, to prove the store returns
		// the originally-produced events rather than re-deciding.
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{"n":999}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil {
		t.Fatalf("second Append: %v", err)
	}
	if !dup2 {
		t.Fatal("second Append with same dedupeKey: duplicate=false, want true")
	}
	if string(second[0].Payload) != string(first[0].Payload) {
		t.Fatalf("duplicate call returned payload %s, want original %s", second[0].Payload, first[0].Payload)
	}

	events, err := store.LoadStream(ctx, "account", "a")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1 (duplicate call must not append again)", len(events))
	}
}

func TestLoadSince_RespectsLimitAndOrdering(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	for i := 0; i < 10; i++ {
		if _, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
			{AggregateType: "account", AggregateID: "a", EventType: "Credited", Payload: []byte(`{}`), ExpectedVersion: eventstore.NoVersionCheck},
		}); err != nil {
			t.Fatalf("Append #%d: %v", i, err)
		}
	}

	page1, err := store.LoadSince(ctx, 0, 4)
	if err != nil {
		t.Fatalf("LoadSince page1: %v", err)
	}
	if len(page1) != 4 {
		t.Fatalf("len(page1) = %d, want 4", len(page1))
	}
	if page1[0].GlobalSeq != 1 || page1[3].GlobalSeq != 4 {
		t.Fatalf("page1 seqs = [%d..%d], want [1..4]", page1[0].GlobalSeq, page1[3].GlobalSeq)
	}

	page2, err := store.LoadSince(ctx, page1[3].GlobalSeq, 100)
	if err != nil {
		t.Fatalf("LoadSince page2: %v", err)
	}
	if len(page2) != 6 {
		t.Fatalf("len(page2) = %d, want 6", len(page2))
	}
	if page2[0].GlobalSeq != 5 {
		t.Fatalf("page2[0].GlobalSeq = %d, want 5", page2[0].GlobalSeq)
	}

	latest, err := store.LatestGlobalSeq(ctx)
	if err != nil {
		t.Fatalf("LatestGlobalSeq: %v", err)
	}
	if latest != 10 {
		t.Fatalf("LatestGlobalSeq = %d, want 10", latest)
	}
}

func TestLoadStream_UnknownStreamReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	events, err := store.LoadStream(ctx, "account", "does-not-exist")
	if err != nil {
		t.Fatalf("LoadStream: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("len(events) = %d, want 0", len(events))
	}
}

func TestLookupDedupe_EmptyKeyNeverMatches(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	_, found, err := store.LookupDedupe(ctx, "")
	if err != nil {
		t.Fatalf("LookupDedupe: %v", err)
	}
	if found {
		t.Fatal("empty dedupe key should never be found")
	}
}

func TestWithClock_UsedForEventTimestamps(t *testing.T) {
	ctx := context.Background()
	fixed := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	store := memory.New().WithClock(func() time.Time { return fixed })

	events, _, err := store.Append(ctx, "", []eventstore.AppendEvent{
		{AggregateType: "account", AggregateID: "a", EventType: "Opened", Payload: []byte(`{}`), ExpectedVersion: eventstore.StreamDoesNotExist},
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !events[0].CreatedAt.Equal(fixed) {
		t.Fatalf("CreatedAt = %v, want %v", events[0].CreatedAt, fixed)
	}
}
