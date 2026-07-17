package bus_test

import (
	"context"
	"testing"

	"streamledger/internal/bus"
	"streamledger/internal/bus/inmemory"
	"streamledger/internal/eventstore"
)

// TestDuplicatingPublisher_RedeliversExtraTimes proves the test-only broker
// simulator actually delivers each event 1+ExtraDeliveries times, which is
// what every "idempotent consumer" test in this project relies on to prove
// real robustness against at-least-once redelivery without a live broker.
func TestDuplicatingPublisher_RedeliversExtraTimes(t *testing.T) {
	ctx := context.Background()
	b := inmemory.New()
	defer func() { _ = b.Close() }()

	var received []eventstore.Event
	unsubscribe, err := b.Subscribe(ctx, func(_ context.Context, msg bus.Message) error {
		received = append(received, msg.Event)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer func() { _ = unsubscribe() }()

	dup := &bus.DuplicatingPublisher{Inner: b, ExtraDeliveries: 3}
	events := []eventstore.Event{{GlobalSeq: 1, AggregateType: "account", AggregateID: "a"}}
	if err := dup.Publish(ctx, events); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if len(received) != 4 { // 1 original + 3 extra
		t.Fatalf("received %d deliveries, want 4", len(received))
	}
	for i, ev := range received {
		if ev.GlobalSeq != 1 {
			t.Fatalf("delivery #%d GlobalSeq = %d, want 1", i, ev.GlobalSeq)
		}
	}
}

func TestInmemoryBus_FanOutToMultipleSubscribers(t *testing.T) {
	ctx := context.Background()
	b := inmemory.New()
	defer func() { _ = b.Close() }()

	var aCount, bCount int
	if _, err := b.Subscribe(ctx, func(_ context.Context, _ bus.Message) error { aCount++; return nil }); err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	if _, err := b.Subscribe(ctx, func(_ context.Context, _ bus.Message) error { bCount++; return nil }); err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	if err := b.Publish(ctx, []eventstore.Event{{GlobalSeq: 1}, {GlobalSeq: 2}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if aCount != 2 || bCount != 2 {
		t.Fatalf("aCount=%d bCount=%d, want 2/2 (both subscribers see both events)", aCount, bCount)
	}
}

func TestInmemoryBus_UnsubscribeStopsDelivery(t *testing.T) {
	ctx := context.Background()
	b := inmemory.New()
	defer func() { _ = b.Close() }()

	count := 0
	unsubscribe, err := b.Subscribe(ctx, func(_ context.Context, _ bus.Message) error { count++; return nil })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := b.Publish(ctx, []eventstore.Event{{GlobalSeq: 1}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if err := b.Publish(ctx, []eventstore.Event{{GlobalSeq: 2}}); err != nil {
		t.Fatalf("Publish after unsubscribe: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 (no delivery after unsubscribe)", count)
	}
}

func TestInmemoryBus_CloseStopsFurtherPublish(t *testing.T) {
	ctx := context.Background()
	b := inmemory.New()

	count := 0
	if _, err := b.Subscribe(ctx, func(_ context.Context, _ bus.Message) error { count++; return nil }); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Publish(ctx, []eventstore.Event{{GlobalSeq: 1}}); err != nil {
		t.Fatalf("Publish after Close: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 (closed bus must not deliver)", count)
	}
}
