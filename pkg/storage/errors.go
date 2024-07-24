package storage

import (
	"errors"
	"fmt"
)

var (
	ErrConfigurationNotFound = errors.New("configuration not found")
)

type Code string

const (
	ConstraintFailed Code = "CONSTRAINT_FAILED"
	ConstraintTXID   Code = "CONSTRAINT_TXID"
	TooManyClient    Code = "TOO_MANY_CLIENT"
)

type Error struct {
	Code          Code
	OriginalError error
}

func (e Error) Is(err error) bool {
	storageErr, ok := err.(*Error)
	if !ok {
		return false
	}
	if storageErr.Code == "" {
		return true
	}
	return storageErr.Code == e.Code
}

func (e Error) Error() string {
	return fmt.Sprintf("%s [%s]", e.OriginalError, e.Code)
}

func NewError(code Code, originalError error) *Error {
	return &Error{
		Code:          code,
		OriginalError: originalError,
	}
}

func IsError(err error) bool {
	return IsErrorCode(err, "")
}

func IsErrorCode(err error, code Code) bool {
	return errors.Is(err, &Error{
		Code: code,
	})
}
