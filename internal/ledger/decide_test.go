package ledger

import (
	"errors"
	"testing"
)

func TestDecideOpenAccount(t *testing.T) {
	tests := []struct {
		name      string
		current   AccountState
		initial   int64
		wantErr   error
		wantEvent AccountOpened
	}{
		{
			name:      "opens a fresh account",
			current:   AccountState{},
			initial:   1000,
			wantEvent: AccountOpened{AccountID: "acct-1", InitialBalanceCents: 1000},
		},
		{
			name:      "allows a zero initial balance",
			current:   AccountState{},
			initial:   0,
			wantEvent: AccountOpened{AccountID: "acct-1", InitialBalanceCents: 0},
		},
		{
			name:    "rejects a negative initial balance",
			current: AccountState{},
			initial: -1,
			wantErr: ErrInvalidAmount,
		},
		{
			name:    "rejects opening an already-opened account",
			current: AccountState{Opened: true},
			initial: 500,
			wantErr: ErrAccountAlreadyOpened,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecideOpenAccount(tc.current, "acct-1", tc.initial)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && got != tc.wantEvent {
				t.Fatalf("event = %+v, want %+v", got, tc.wantEvent)
			}
		})
	}
}

func TestDecideDeposit(t *testing.T) {
	tests := []struct {
		name    string
		current AccountState
		amount  int64
		wantErr error
	}{
		{name: "deposits into an open account", current: AccountState{Opened: true, BalanceCents: 100}, amount: 50},
		{name: "rejects deposit to unopened account", current: AccountState{}, amount: 50, wantErr: ErrAccountNotOpen},
		{name: "rejects zero amount", current: AccountState{Opened: true}, amount: 0, wantErr: ErrInvalidAmount},
		{name: "rejects negative amount", current: AccountState{Opened: true}, amount: -10, wantErr: ErrInvalidAmount},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			evt, err := DecideDeposit(tc.current, tc.amount, "test")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && evt.AmountCents != tc.amount {
				t.Fatalf("AmountCents = %d, want %d", evt.AmountCents, tc.amount)
			}
		})
	}
}

func TestDecideWithdraw(t *testing.T) {
	tests := []struct {
		name    string
		current AccountState
		amount  int64
		wantErr error
	}{
		{name: "withdraws within balance", current: AccountState{Opened: true, BalanceCents: 100}, amount: 100},
		{name: "rejects withdraw from unopened account", current: AccountState{}, amount: 10, wantErr: ErrAccountNotOpen},
		{name: "rejects zero amount", current: AccountState{Opened: true, BalanceCents: 100}, amount: 0, wantErr: ErrInvalidAmount},
		{name: "rejects negative amount", current: AccountState{Opened: true, BalanceCents: 100}, amount: -5, wantErr: ErrInvalidAmount},
		{
			name:    "rejects withdrawal exceeding balance (no negative balance ever)",
			current: AccountState{Opened: true, BalanceCents: 99},
			amount:  100,
			wantErr: ErrInsufficientFunds,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecideWithdraw(tc.current, tc.amount, "test")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestDecideAdjustInventory(t *testing.T) {
	tests := []struct {
		name    string
		current InventoryState
		delta   int64
		wantErr error
	}{
		{name: "positive restock", current: InventoryState{AvailableQuantity: 10}, delta: 5},
		{name: "negative shrinkage within stock", current: InventoryState{AvailableQuantity: 10}, delta: -10},
		{name: "rejects zero delta", current: InventoryState{AvailableQuantity: 10}, delta: 0, wantErr: ErrInvalidQuantity},
		{
			name:    "rejects shrinkage below zero available",
			current: InventoryState{AvailableQuantity: 5},
			delta:   -6,
			wantErr: ErrInsufficientStock,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecideAdjustInventory(tc.current, "item-1", tc.delta, "test")
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestDecideReserveAndReleaseStock(t *testing.T) {
	t.Run("reserve within available", func(t *testing.T) {
		evt, err := DecideReserveStock(InventoryState{AvailableQuantity: 10}, "item-1", 4, "order-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if evt.Quantity != 4 {
			t.Fatalf("Quantity = %d, want 4", evt.Quantity)
		}
	})
	t.Run("reserve rejects exceeding available", func(t *testing.T) {
		_, err := DecideReserveStock(InventoryState{AvailableQuantity: 3}, "item-1", 4, "order-1")
		if !errors.Is(err, ErrInsufficientStock) {
			t.Fatalf("err = %v, want ErrInsufficientStock", err)
		}
	})
	t.Run("reserve rejects non-positive quantity", func(t *testing.T) {
		_, err := DecideReserveStock(InventoryState{AvailableQuantity: 3}, "item-1", 0, "order-1")
		if !errors.Is(err, ErrInvalidQuantity) {
			t.Fatalf("err = %v, want ErrInvalidQuantity", err)
		}
	})
	t.Run("release within reserved", func(t *testing.T) {
		evt, err := DecideReleaseStock(InventoryState{ReservedQuantity: 4}, "item-1", 4, "order-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if evt.Quantity != 4 {
			t.Fatalf("Quantity = %d, want 4", evt.Quantity)
		}
	})
	t.Run("release rejects exceeding reserved", func(t *testing.T) {
		_, err := DecideReleaseStock(InventoryState{ReservedQuantity: 2}, "item-1", 4, "order-1")
		if !errors.Is(err, ErrInsufficientStock) {
			t.Fatalf("err = %v, want ErrInsufficientStock", err)
		}
	})
}
