package ledger

import "errors"

// Business-rule errors returned by the Decide* pure functions. These are
// distinct from eventstore.ErrVersionConflict, which is a concurrency error
// rather than a business-rule violation.
var (
	ErrInvalidAmount        = errors.New("ledger: amount must be positive")
	ErrInvalidQuantity      = errors.New("ledger: quantity must be positive")
	ErrAccountAlreadyOpened = errors.New("ledger: account already opened")
	ErrAccountNotOpen       = errors.New("ledger: account has not been opened")
	ErrInsufficientFunds    = errors.New("ledger: insufficient funds")
	ErrInsufficientStock    = errors.New("ledger: insufficient available stock")
	ErrSameAccountTransfer  = errors.New("ledger: cannot transfer to the same account")
)
