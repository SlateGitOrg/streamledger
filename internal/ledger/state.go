package ledger

import (
	"encoding/json"
	"fmt"

	"streamledger/internal/eventstore"
)

// AccountState is the folded result of replaying an account's event stream.
type AccountState struct {
	AccountID    string
	Version      int64 // 0 == stream does not exist yet
	Opened       bool
	BalanceCents int64
}

// InventoryState is the folded result of replaying an inventory item's
// event stream.
type InventoryState struct {
	ItemID            string
	Version           int64
	AvailableQuantity int64
	ReservedQuantity  int64
}

// FoldAccount replays an account's events into its current state. It is
// pure and deterministic: same events in, same state out, every time.
func FoldAccount(accountID string, events []eventstore.Event) (AccountState, error) {
	st := AccountState{AccountID: accountID}
	for _, e := range events {
		st.Version = e.Version
		switch e.EventType {
		case EventAccountOpened:
			var payload AccountOpened
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.Opened = true
			st.BalanceCents = payload.InitialBalanceCents
		case EventFundsDeposited:
			var payload FundsDeposited
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.BalanceCents += payload.AmountCents
		case EventFundsWithdrawn:
			var payload FundsWithdrawn
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.BalanceCents -= payload.AmountCents
		default:
			return st, fmt.Errorf("unknown account event type %q", e.EventType)
		}
	}
	return st, nil
}

// FoldInventory replays an inventory item's events into its current state.
func FoldInventory(itemID string, events []eventstore.Event) (InventoryState, error) {
	st := InventoryState{ItemID: itemID}
	for _, e := range events {
		st.Version = e.Version
		switch e.EventType {
		case EventInventoryAdjusted:
			var payload InventoryAdjusted
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.AvailableQuantity += payload.DeltaQuantity
		case EventStockReserved:
			var payload StockReserved
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.AvailableQuantity -= payload.Quantity
			st.ReservedQuantity += payload.Quantity
		case EventStockReleased:
			var payload StockReleased
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return st, fmt.Errorf("decode %s: %w", e.EventType, err)
			}
			st.ReservedQuantity -= payload.Quantity
			st.AvailableQuantity += payload.Quantity
		default:
			return st, fmt.Errorf("unknown inventory event type %q", e.EventType)
		}
	}
	return st, nil
}
