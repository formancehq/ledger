package state

import (
	"github.com/pkg/errors"
)

var (
	ErrPastTransaction = errors.New("cannot pass a timestamp prior to the last transaction")
	ErrConflictError   = errors.New("conflict error")
)

func IsPastTransactionError(err error) bool {
	return errors.Is(err, ErrPastTransaction)
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflictError)
}
