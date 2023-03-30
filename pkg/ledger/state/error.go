package state

import (
	"github.com/pkg/errors"
)

var (
	ErrPastTransaction = errors.New("cannot pass a timestamp prior to the last transaction")
	ErrConflictError   = errors.New("conflict error")
	ErrStorage         = errors.New("storage error")
)

func IsPastTransaction(err error) bool {
	return errors.Is(err, ErrPastTransaction)
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflictError)
}

func IsStorageError(err error) bool {
	return errors.Is(err, ErrStorage)
}
