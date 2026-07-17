package projections

import (
	"context"
	"encoding/json"
	"fmt"

	"streamledger/internal/eventstore"
	"streamledger/internal/ledger"
)

// AccountApplier folds account (balance) events into AccountView rows. The
// "balances" projection uses this.
type AccountApplier struct {
	Store ReadModelStore
}

func (a *AccountApplier) Relevant(event eventstore.Event) bool {
	return event.AggregateType == ledger.AggregateTypeAccount
}

func (a *AccountApplier) Apply(ctx context.Context, event eventstore.Event) error {
	view, _, err := a.Store.GetAccount(ctx, event.AggregateID)
	if err != nil {
		return err
	}
	view.AccountID = event.AggregateID

	switch event.EventType {
	case ledger.EventAccountOpened:
		var payload ledger.AccountOpened
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("account projection: decode %s: %w", event.EventType, err)
		}
		view.BalanceCents = payload.InitialBalanceCents
	case ledger.EventFundsDeposited:
		var payload ledger.FundsDeposited
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("account projection: decode %s: %w", event.EventType, err)
		}
		view.BalanceCents += payload.AmountCents
	case ledger.EventFundsWithdrawn:
		var payload ledger.FundsWithdrawn
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("account projection: decode %s: %w", event.EventType, err)
		}
		view.BalanceCents -= payload.AmountCents
	default:
		return fmt.Errorf("account projection: unexpected event type %q", event.EventType)
	}

	view.Version = event.Version
	view.UpdatedAt = event.CreatedAt
	return a.Store.UpsertAccount(ctx, view)
}

var _ EventApplier = (*AccountApplier)(nil)
