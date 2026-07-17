// Package ledger implements the two event-sourced aggregates StreamLedger
// ships with: accounts (a balance ledger) and inventory items (stock
// counts). Both are plain, deterministic, side-effect-free state machines:
// given the events so far and a command, they decide what new events (if
// any) should be appended. All I/O (persistence, concurrency control,
// idempotency) lives in the eventstore and command-handler layers so this
// package stays trivially unit-testable.
package ledger

const (
	// AggregateTypeAccount identifies the balance-ledger aggregate stream.
	AggregateTypeAccount = "account"
	// AggregateTypeInventoryItem identifies the inventory aggregate stream.
	AggregateTypeInventoryItem = "inventory_item"
)

// Event type names stored in eventstore.Event.EventType and used as the
// discriminator when decoding JSON payloads.
const (
	EventAccountOpened     = "AccountOpened"
	EventFundsDeposited    = "FundsDeposited"
	EventFundsWithdrawn    = "FundsWithdrawn"
	EventInventoryAdjusted = "InventoryAdjusted"
	EventStockReserved     = "StockReserved"
	EventStockReleased     = "StockReleased"
)

// AccountOpened is the first event of every account stream.
type AccountOpened struct {
	AccountID           string `json:"account_id"`
	InitialBalanceCents int64  `json:"initial_balance_cents"`
}

// FundsDeposited increases an account's balance.
type FundsDeposited struct {
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
	// Reason is optional context, e.g. "transfer:<correlation_id>".
	Reason string `json:"reason,omitempty"`
}

// FundsWithdrawn decreases an account's balance. The command handler
// guarantees AmountCents never exceeds the balance at the time of decision.
type FundsWithdrawn struct {
	AccountID   string `json:"account_id"`
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason,omitempty"`
}

// InventoryAdjusted is a signed stock-level change (restock, shrinkage,
// manual correction) that is not part of the reserve/release protocol.
type InventoryAdjusted struct {
	ItemID        string `json:"item_id"`
	DeltaQuantity int64  `json:"delta_quantity"`
	Reason        string `json:"reason,omitempty"`
}

// StockReserved moves quantity from available to reserved (e.g. an order
// was placed but not yet fulfilled).
type StockReserved struct {
	ItemID   string `json:"item_id"`
	Quantity int64  `json:"quantity"`
	OrderID  string `json:"order_id,omitempty"`
}

// StockReleased moves quantity from reserved back to available (order
// cancelled) — it does not fulfil/consume stock, see notes in README.
type StockReleased struct {
	ItemID   string `json:"item_id"`
	Quantity int64  `json:"quantity"`
	OrderID  string `json:"order_id,omitempty"`
}
