package command

import (
	"github.com/pkg/errors"
)

var (
	ErrNoPostings        = errors.New("transaction has no postings")
	ErrNoScript          = errors.New("no script")
	ErrCompilationFailed = errors.New("compilation failed")
	ErrVM                = errors.New("vm error")
	ErrState             = errors.New("state error")
	ErrValidation        = errors.New("validation error")
	ErrAlreadyReverted   = errors.New("transaction already reverted")
	ErrRevertOccurring   = errors.New("revert already occurring")
	ErrPastTransaction   = errors.New("cannot pass a timestamp prior to the last transaction")
	ErrConflictError     = errors.New("conflict error")
)

func IsNoScriptError(err error) bool {
	return errors.Is(err, ErrNoScript)
}

func IsNoPostingsError(err error) bool {
	return errors.Is(err, ErrNoPostings)
}

func IsCompilationFailedError(err error) bool {
	return errors.Is(err, ErrCompilationFailed)
}

func IsValidationError(err error) bool {
	return errors.Is(err, ErrValidation)
}

func IsPastTransactionError(err error) bool {
	return errors.Is(err, ErrPastTransaction)
}

func IsConflictError(err error) bool {
	return errors.Is(err, ErrConflictError)
}
