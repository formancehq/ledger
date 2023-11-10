package command

import (
	"fmt"

	"github.com/pkg/errors"
)

const (
	ErrSaveMetaCodeTransactionNotFound = "TRANSACTION_NOT_FOUND"
)

type errSaveMeta struct {
	code string
}

func (e *errSaveMeta) Error() string {
	return fmt.Sprintf("invalid transaction: %s", e.code)
}

func (e *errSaveMeta) Is(err error) bool {
	_, ok := err.(*errSaveMeta)
	return ok
}

func newErrSaveMeta(code string) *errSaveMeta {
	return &errSaveMeta{
		code: code,
	}
}

func newErrSaveMetadataTransactionNotFound() *errSaveMeta {
	return newErrSaveMeta(ErrSaveMetaCodeTransactionNotFound)
}

func IsSaveMetaError(err error, code string) bool {
	e := &errSaveMeta{}
	if errors.As(err, &e) {
		return e.code == code
	}

	return false
}

const (
	ErrDeleteMetaCodeTransactionNotFound = "TRANSACTION_NOT_FOUND"
)

type errDeleteMeta struct {
	code string
}

func (e *errDeleteMeta) Error() string {
	return fmt.Sprintf("invalid transaction: %s", e.code)
}

func (e *errDeleteMeta) Is(err error) bool {
	_, ok := err.(*errDeleteMeta)
	return ok
}

func newErrDeleteMeta(code string) *errDeleteMeta {
	return &errDeleteMeta{
		code: code,
	}
}

func IsDeleteMetaError(err error, code string) bool {
	e := &errDeleteMeta{}
	if errors.As(err, &e) {
		return e.code == code
	}

	return false
}

func newErrDeleteMetadataTransactionNotFound() *errDeleteMeta {
	return newErrDeleteMeta(ErrDeleteMetaCodeTransactionNotFound)
}

type errRevert struct {
	code string
}

func (e *errRevert) Error() string {
	return fmt.Sprintf("invalid transaction: %s", e.code)
}

func (e *errRevert) Is(err error) bool {
	_, ok := err.(*errRevert)
	return ok
}

func NewErrRevert(code string) *errRevert {
	return &errRevert{
		code: code,
	}
}

const (
	ErrRevertTransactionCodeAlreadyReverted = "ALREADY_REVERTED"
	ErrRevertTransactionCodeOccurring       = "REVERT_OCCURRING"
	ErrRevertTransactionCodeNotFound        = "NOT_FOUND"
)

func NewErrRevertTransactionOccurring() *errRevert {
	return NewErrRevert(ErrRevertTransactionCodeOccurring)
}

func NewErrRevertTransactionAlreadyReverted() *errRevert {
	return NewErrRevert(ErrRevertTransactionCodeAlreadyReverted)
}

func NewErrRevertTransactionNotFound() *errRevert {
	return NewErrRevert(ErrRevertTransactionCodeNotFound)
}

func IsRevertError(err error, code string) bool {
	e := &errRevert{}
	if errors.As(err, &e) {
		return e.code == code
	}

	return false
}

type errInvalidTransaction struct {
	code string
	err  error
}

func (e *errInvalidTransaction) Error() string {
	if e.err == nil {
		return fmt.Sprintf("invalid transaction: %s", e.code)
	}
	return fmt.Sprintf("invalid transaction: %s (%s)", e.code, e.err)
}

func (e *errInvalidTransaction) Is(err error) bool {
	_, ok := err.(*errInvalidTransaction)
	return ok
}

func (e *errInvalidTransaction) Cause() error {
	return e.err
}

func NewErrInvalidTransaction(code string, err error) *errInvalidTransaction {
	return &errInvalidTransaction{
		code: code,
		err:  err,
	}
}

const (
	ErrInvalidTransactionCodeCompilationFailed = "COMPILATION_FAILED"
	ErrInvalidTransactionCodeNoScript          = "NO_SCRIPT"
	ErrInvalidTransactionCodeNoPostings        = "NO_POSTINGS"
	ErrInvalidTransactionCodeConflict          = "CONFLICT"
)

func NewErrCompilationFailed(err error) *errInvalidTransaction {
	return NewErrInvalidTransaction(ErrInvalidTransactionCodeCompilationFailed, err)
}

func NewErrNoScript() *errInvalidTransaction {
	return NewErrInvalidTransaction(ErrInvalidTransactionCodeNoScript, nil)
}

func NewErrNoPostings() *errInvalidTransaction {
	return NewErrInvalidTransaction(ErrInvalidTransactionCodeNoPostings, nil)
}

func NewErrConflict() *errInvalidTransaction {
	return NewErrInvalidTransaction(ErrInvalidTransactionCodeConflict, nil)
}

func IsInvalidTransactionError(err error, code string) bool {
	e := &errInvalidTransaction{}
	if errors.As(err, &e) {
		return e.code == code
	}

	return false
}

type errMachine struct {
	err error
}

func (e *errMachine) Error() string {
	return errors.Wrap(e.err, "running numscript").Error()
}

func (e *errMachine) Is(err error) bool {
	_, ok := err.(*errMachine)
	return ok
}

func (e *errMachine) Unwrap() error {
	return e.err
}

func NewErrMachine(err error) *errMachine {
	return &errMachine{
		err: err,
	}
}

func IsErrMachine(err error) bool {
	return errors.Is(err, &errMachine{})
}
