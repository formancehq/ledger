package storage

import (
	"fmt"

	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrStoreNotInitialized = errors.New("store not initialized")
)

var (
	// Specific pq sql errors
	ErrConstraintFailed = pq.ErrorCode("23505")
	ErrTooManyClient    = pq.ErrorCode("53300")
)

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

type Error struct {
	code pq.ErrorCode
	err  error
}

func NewError(code pq.ErrorCode, err error) *Error {
	return &Error{
		code: code,
		err:  err,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("[%s] %s", e.code, e.err)
}

func IsError(err error) bool {
	return errors.Is(err, &Error{})
}
