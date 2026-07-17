package ledger

// This file contains the pure decision logic of both aggregates: given the
// current folded state and a command's arguments, either return the event
// that should be appended or a business-rule error. No I/O, no randomness,
// no clocks — which is what makes these trivial to table-drive-test.

// DecideOpenAccount validates opening a brand new account.
func DecideOpenAccount(current AccountState, accountID string, initialBalanceCents int64) (AccountOpened, error) {
	if current.Opened {
		return AccountOpened{}, ErrAccountAlreadyOpened
	}
	if initialBalanceCents < 0 {
		return AccountOpened{}, ErrInvalidAmount
	}
	return AccountOpened{AccountID: accountID, InitialBalanceCents: initialBalanceCents}, nil
}

// DecideDeposit validates a deposit against the current account state.
func DecideDeposit(current AccountState, amountCents int64, reason string) (FundsDeposited, error) {
	if !current.Opened {
		return FundsDeposited{}, ErrAccountNotOpen
	}
	if amountCents <= 0 {
		return FundsDeposited{}, ErrInvalidAmount
	}
	return FundsDeposited{AccountID: current.AccountID, AmountCents: amountCents, Reason: reason}, nil
}

// DecideWithdraw validates a withdrawal against the current account state,
// enforcing that the balance can never go negative.
func DecideWithdraw(current AccountState, amountCents int64, reason string) (FundsWithdrawn, error) {
	if !current.Opened {
		return FundsWithdrawn{}, ErrAccountNotOpen
	}
	if amountCents <= 0 {
		return FundsWithdrawn{}, ErrInvalidAmount
	}
	if amountCents > current.BalanceCents {
		return FundsWithdrawn{}, ErrInsufficientFunds
	}
	return FundsWithdrawn{AccountID: current.AccountID, AmountCents: amountCents, Reason: reason}, nil
}

// DecideAdjustInventory validates a (possibly negative) stock adjustment.
func DecideAdjustInventory(current InventoryState, itemID string, delta int64, reason string) (InventoryAdjusted, error) {
	if delta == 0 {
		return InventoryAdjusted{}, ErrInvalidQuantity
	}
	if delta < 0 && current.AvailableQuantity+delta < 0 {
		return InventoryAdjusted{}, ErrInsufficientStock
	}
	return InventoryAdjusted{ItemID: itemID, DeltaQuantity: delta, Reason: reason}, nil
}

// DecideReserveStock validates reserving stock for an order.
func DecideReserveStock(current InventoryState, itemID string, quantity int64, orderID string) (StockReserved, error) {
	if quantity <= 0 {
		return StockReserved{}, ErrInvalidQuantity
	}
	if quantity > current.AvailableQuantity {
		return StockReserved{}, ErrInsufficientStock
	}
	return StockReserved{ItemID: itemID, Quantity: quantity, OrderID: orderID}, nil
}

// DecideReleaseStock validates releasing previously reserved stock.
func DecideReleaseStock(current InventoryState, itemID string, quantity int64, orderID string) (StockReleased, error) {
	if quantity <= 0 {
		return StockReleased{}, ErrInvalidQuantity
	}
	if quantity > current.ReservedQuantity {
		return StockReleased{}, ErrInsufficientStock
	}
	return StockReleased{ItemID: itemID, Quantity: quantity, OrderID: orderID}, nil
}
