package ledger

import (
	"fmt"
	"github.com/pkg/errors"
)

var ErrCommitError = errors.New("commit error")

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
	Reference string
}

func (e ConflictError) Error() string {
	return fmt.Sprintf("conflict error on reference '%s'", e.Reference)
}

func (e ConflictError) Is(err error) bool {
	_, ok := err.(*ConflictError)
	return ok
}

func NewConflictError(ref string) *ConflictError {
	return &ConflictError{
		Reference: ref,
	}
}

func IsConflictError(err error) bool {
	return errors.Is(err, &ConflictError{})
}

const (
	ScriptErrorInsufficientFund  = "INSUFFICIENT_FUND"
	ScriptErrorCompilationFailed = "COMPILATION_FAILED"
	ScriptErrorNoScript          = "NO_SCRIPT"
)

type ScriptError struct {
	Code    string
	Message string
}

func (e ScriptError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e ScriptError) Is(err error) bool {
	eerr, ok := err.(*ScriptError)
	if !ok {
		return false
	}
	return e.Code == eerr.Code
}

func IsScriptError(err error) bool {
	return errors.Is(err, &ScriptError{})
}

func NewScriptError(code string, message string) *ScriptError {
	return &ScriptError{
		Code:    code,
		Message: message,
	}
}
