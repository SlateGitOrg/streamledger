package ledger

import (
	"encoding/json"
	"testing"

	"streamledger/internal/eventstore"
)

func mustEvent(t *testing.T, aggregateType, eventType string, version int64, payload any) eventstore.Event {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return eventstore.Event{
		AggregateType: aggregateType,
		Version:       version,
		EventType:     eventType,
		Payload:       b,
	}
}

func TestFoldAccount(t *testing.T) {
	events := []eventstore.Event{
		mustEvent(t, AggregateTypeAccount, EventAccountOpened, 1, AccountOpened{AccountID: "acct-1", InitialBalanceCents: 100}),
		mustEvent(t, AggregateTypeAccount, EventFundsDeposited, 2, FundsDeposited{AccountID: "acct-1", AmountCents: 50}),
		mustEvent(t, AggregateTypeAccount, EventFundsWithdrawn, 3, FundsWithdrawn{AccountID: "acct-1", AmountCents: 30}),
	}

	st, err := FoldAccount("acct-1", events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !st.Opened {
		t.Fatal("expected account to be opened")
	}
	if st.BalanceCents != 120 {
		t.Fatalf("BalanceCents = %d, want 120 (100+50-30)", st.BalanceCents)
	}
	if st.Version != 3 {
		t.Fatalf("Version = %d, want 3", st.Version)
	}
}

func TestFoldAccount_EmptyStream(t *testing.T) {
	st, err := FoldAccount("acct-1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Opened || st.BalanceCents != 0 || st.Version != 0 {
		t.Fatalf("expected zero-value state, got %+v", st)
	}
}

func TestFoldAccount_UnknownEventType(t *testing.T) {
	events := []eventstore.Event{
		mustEvent(t, AggregateTypeAccount, "SomeFutureEvent", 1, map[string]string{}),
	}
	if _, err := FoldAccount("acct-1", events); err == nil {
		t.Fatal("expected error for unknown event type, got nil")
	}
}

func TestFoldAccount_CorruptPayload(t *testing.T) {
	events := []eventstore.Event{
		{AggregateType: AggregateTypeAccount, Version: 1, EventType: EventAccountOpened, Payload: []byte("not json")},
	}
	if _, err := FoldAccount("acct-1", events); err == nil {
		t.Fatal("expected decode error, got nil")
	}
}

func TestFoldInventory(t *testing.T) {
	events := []eventstore.Event{
		mustEvent(t, AggregateTypeInventoryItem, EventInventoryAdjusted, 1, InventoryAdjusted{ItemID: "item-1", DeltaQuantity: 20}),
		mustEvent(t, AggregateTypeInventoryItem, EventStockReserved, 2, StockReserved{ItemID: "item-1", Quantity: 5, OrderID: "order-1"}),
		mustEvent(t, AggregateTypeInventoryItem, EventStockReleased, 3, StockReleased{ItemID: "item-1", Quantity: 2, OrderID: "order-1"}),
	}

	st, err := FoldInventory("item-1", events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 20 stocked -> 5 reserved (15 avail / 5 reserved) -> release 2 (17 avail / 3 reserved)
	if st.AvailableQuantity != 17 {
		t.Fatalf("AvailableQuantity = %d, want 17", st.AvailableQuantity)
	}
	if st.ReservedQuantity != 3 {
		t.Fatalf("ReservedQuantity = %d, want 3", st.ReservedQuantity)
	}
	if st.Version != 3 {
		t.Fatalf("Version = %d, want 3", st.Version)
	}
}

func TestFoldInventory_UnknownEventType(t *testing.T) {
	events := []eventstore.Event{
		mustEvent(t, AggregateTypeInventoryItem, "SomeFutureEvent", 1, map[string]string{}),
	}
	if _, err := FoldInventory("item-1", events); err == nil {
		t.Fatal("expected error for unknown event type, got nil")
	}
}
