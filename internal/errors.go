package ledger

import "github.com/pkg/errors"

var ErrInsufficientFund = errors.New("account had insufficient funds")
