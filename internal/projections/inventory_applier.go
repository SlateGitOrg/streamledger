package projections

import (
	"context"
	"encoding/json"
	"fmt"

	"streamledger/internal/eventstore"
	"streamledger/internal/ledger"
)

// InventoryApplier folds inventory events into InventoryView rows. The
// "inventory" projection uses this.
type InventoryApplier struct {
	Store ReadModelStore
}

func (a *InventoryApplier) Relevant(event eventstore.Event) bool {
	return event.AggregateType == ledger.AggregateTypeInventoryItem
}

func (a *InventoryApplier) Apply(ctx context.Context, event eventstore.Event) error {
	view, _, err := a.Store.GetInventory(ctx, event.AggregateID)
	if err != nil {
		return err
	}
	view.ItemID = event.AggregateID

	switch event.EventType {
	case ledger.EventInventoryAdjusted:
		var payload ledger.InventoryAdjusted
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("inventory projection: decode %s: %w", event.EventType, err)
		}
		view.AvailableQuantity += payload.DeltaQuantity
	case ledger.EventStockReserved:
		var payload ledger.StockReserved
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("inventory projection: decode %s: %w", event.EventType, err)
		}
		view.AvailableQuantity -= payload.Quantity
		view.ReservedQuantity += payload.Quantity
	case ledger.EventStockReleased:
		var payload ledger.StockReleased
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("inventory projection: decode %s: %w", event.EventType, err)
		}
		view.ReservedQuantity -= payload.Quantity
		view.AvailableQuantity += payload.Quantity
	default:
		return fmt.Errorf("inventory projection: unexpected event type %q", event.EventType)
	}

	view.Version = event.Version
	view.UpdatedAt = event.CreatedAt
	return a.Store.UpsertInventory(ctx, view)
}

var _ EventApplier = (*InventoryApplier)(nil)
