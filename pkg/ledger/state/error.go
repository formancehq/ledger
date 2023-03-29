package state

import (
	"fmt"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/pkg/errors"
)

type ErrPastTransaction struct {
	MoreRecent core.Time
	Asked      core.Time
}

func (e ErrPastTransaction) Error() string {
	return fmt.Sprintf(
		"cannot pass a timestamp prior to the last transaction: %s (passed) is %s before %s (last)",
		e.Asked.Format(time.RFC3339Nano),
		e.MoreRecent.Sub(e.Asked),
		e.MoreRecent.Format(time.RFC3339Nano))
}

func (e ErrPastTransaction) Is(err error) bool {
	_, ok := err.(*ErrPastTransaction)
	return ok
}

func newErrPastTransaction(moreRecent, asked core.Time) ErrPastTransaction {
	return ErrPastTransaction{
		MoreRecent: moreRecent,
		Asked:      asked,
	}
}

func IsPastTransaction(err error) bool {
	return errors.Is(err, &ErrPastTransaction{})
}

type ErrConflictError struct {
	msg string
}

func (e ErrConflictError) Error() string {
	return fmt.Sprintf("conflict error: %s", e.msg)
}

func (e ErrConflictError) Is(err error) bool {
	_, ok := err.(*ErrConflictError)
	return ok
}

func NewConflictError(msg string) *ErrConflictError {
	return &ErrConflictError{
		msg: msg,
	}
}

func IsConflictError(err error) bool {
	return errors.Is(err, &ErrConflictError{})
}
