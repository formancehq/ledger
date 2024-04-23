package errorsutils

import (
	"errors"
	"fmt"
)

type ErrorWithExitCode struct {
	Err      error
	ExitCode int
}

func (e ErrorWithExitCode) Unwrap() error {
	return e.Err
}

func (e ErrorWithExitCode) Error() string {
	return fmt.Sprintf("error with exit code '%v': %d", e.Err, e.ExitCode)
}

func (e ErrorWithExitCode) Is(err error) bool {
	_, ok := err.(ErrorWithExitCode)
	return ok
}

func IsErrorWithExitCode(err error) bool {
	return errors.Is(err, ErrorWithExitCode{})
}

func NewErrorWithExitCode(err error, exitCode int) *ErrorWithExitCode {
	return &ErrorWithExitCode{
		Err:      err,
		ExitCode: exitCode,
	}
}
