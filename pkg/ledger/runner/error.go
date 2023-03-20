package runner

import (
	"fmt"

	"github.com/pkg/errors"
)

type TransactionCommitError struct {
	TXIndex int   `json:"index"`
	Err     error `json:"error"`
}

func (e TransactionCommitError) Unwrap() error {
	return e.Err
}

func (e TransactionCommitError) Error() string {
	return errors.Wrapf(e.Err, "processing tx %d", e.TXIndex).Error()
}

func (e TransactionCommitError) Is(err error) bool {
	_, ok := err.(*TransactionCommitError)
	return ok
}

func NewTransactionCommitError(txIndex int, err error) *TransactionCommitError {
	return &TransactionCommitError{
		TXIndex: txIndex,
		Err:     err,
	}
}

func IsTransactionCommitError(err error) bool {
	return errors.Is(err, &TransactionCommitError{})
}

type InsufficientFundError struct {
	Asset string
}

func (e InsufficientFundError) Error() string {
	return fmt.Sprintf("balance.insufficient.%s", e.Asset)
}

func (e InsufficientFundError) Is(err error) bool {
	_, ok := err.(*InsufficientFundError)
	return ok
}

func NewInsufficientFundError(asset string) *InsufficientFundError {
	return &InsufficientFundError{
		Asset: asset,
	}
}

func IsInsufficientFundError(err error) bool {
	return errors.Is(err, &InsufficientFundError{})
}

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

type ConflictError struct {
	msg string
}

func (e ConflictError) Error() string {
	return fmt.Sprintf("conflict error: %s", e.msg)
}

func (e ConflictError) Is(err error) bool {
	_, ok := err.(*ConflictError)
	return ok
}

func NewConflictError(msg string) *ConflictError {
	return &ConflictError{
		msg: msg,
	}
}

func IsConflictError(err error) bool {
	return errors.Is(err, &ConflictError{})
}

type LockError struct {
	Err error
}

func (e LockError) Error() string {
	return e.Err.Error()
}

func (e LockError) Is(err error) bool {
	_, ok := err.(*LockError)
	return ok
}

func IsLockError(err error) bool {
	return errors.Is(err, &LockError{})
}

func NewLockError(err error) *LockError {
	return &LockError{
		Err: err,
	}
}

type NotFoundError struct {
	Msg string
}

func (v NotFoundError) Error() string {
	return v.Msg
}

func (v NotFoundError) Is(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}

func NewNotFoundError(msg string) *NotFoundError {
	return &NotFoundError{
		Msg: msg,
	}
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, &NotFoundError{})
}
