package ledger

import (
	"fmt"
	"github.com/pkg/errors"
)

var ErrCommitError = errors.New("commit error")

type UnavailableStoreError struct {
	Err error
}

func (e UnavailableStoreError) Error() string {
	return fmt.Sprintf("unavailable store: %s", e.Err)
}

func (e UnavailableStoreError) Unwrap() error {
	return e.Err
}

func (e UnavailableStoreError) Is(err error) bool {
	_, ok := err.(*UnavailableStoreError)
	return ok
}

func NewUnavailableStoreError(err error) *UnavailableStoreError {
	return &UnavailableStoreError{
		Err: err,
	}
}

func IsUnavailableStoreError(err error) bool {
	return errors.Is(err, &UnavailableStoreError{})
}

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

func NewTransactionCommitError(txIndex int, err error) *TransactionCommitError {
	return &TransactionCommitError{
		TXIndex: txIndex,
		Err:     err,
	}
}

type InsufficientFundError struct {
	Asset string
}

func (e InsufficientFundError) Error() string {
	return fmt.Sprintf("balance.insufficient.%s", e.Asset)
}

func NewInsufficientFundError(asset string) *InsufficientFundError {
	return &InsufficientFundError{
		Asset: asset,
	}
}

type ValidationError struct {
	Msg string
}

func (v ValidationError) Error() string {
	return v.Msg
}

func NewValidationError(msg string) *ValidationError {
	return &ValidationError{
		Msg: msg,
	}
}

type ConflictError struct {
	Reference string
}

func (e ConflictError) Error() string {
	return fmt.Sprintf("conflict error on reference '%s'", e.Reference)
}

func NewConflictError(ref string) *ConflictError {
	return &ConflictError{
		Reference: ref,
	}
}
