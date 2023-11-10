package engine

import (
	"fmt"

	"github.com/pkg/errors"
)

type storageError struct {
	err error
	msg string
}

func (e *storageError) Error() string {
	return fmt.Sprintf("%s: %s", e.msg, e.err)
}

func (e *storageError) Is(err error) bool {
	_, ok := err.(*storageError)
	return ok
}

func (e *storageError) Unwrap() error {
	return e.err
}

func newStorageError(err error, msg string) error {
	if err == nil {
		return nil
	}
	return &storageError{
		err: err,
		msg: msg,
	}
}

func IsStorageError(err error) bool {
	return errors.Is(err, &storageError{})
}

type commandError struct {
	err error
}

func (e *commandError) Error() string {
	return e.err.Error()
}

func (e *commandError) Is(err error) bool {
	_, ok := err.(*commandError)
	return ok
}

func (e *commandError) Unwrap() error {
	return e.err
}

func (e *commandError) Cause() error {
	return e.err
}

func NewCommandError(err error) error {
	if err == nil {
		return nil
	}
	return &commandError{
		err: err,
	}
}

func IsCommandError(err error) bool {
	return errors.Is(err, &commandError{})
}
