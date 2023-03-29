package ledger

import (
	"github.com/pkg/errors"
)

type ValidationError struct {
	Msg string
}

func (v ValidationError) Error() string {
	return v.Msg
}

func (v ValidationError) Is(err error) bool {
	_, ok := err.(*ValidationError)
	return ok
}

func NewValidationError(msg string) *ValidationError {
	return &ValidationError{
		Msg: msg,
	}
}

func IsValidationError(err error) bool {
	return errors.Is(err, &ValidationError{})
}

type ErrNotFound struct {
	Msg string
}

func (v ErrNotFound) Error() string {
	return v.Msg
}

func (v ErrNotFound) Is(err error) bool {
	_, ok := err.(*ErrNotFound)
	return ok
}

func NewErrNotFound(msg string) *ErrNotFound {
	return &ErrNotFound{
		Msg: msg,
	}
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, &ErrNotFound{})
}
