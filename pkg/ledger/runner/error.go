package runner

import (
	"github.com/pkg/errors"
)

var (
	ErrNoPostings        = errors.New("transaction has no postings")
	ErrNoScript          = errors.New("no script")
	ErrCompilationFailed = errors.New("compilation failed")
	ErrVM                = errors.New("vm error")
	ErrState             = errors.New("state error")
)

func IsNoScriptError(err error) bool {
	return errors.Is(err, ErrNoScript)
}

func IsNoPostingsError(err error) bool {
	return errors.Is(err, ErrNoPostings)
}

func IsStateError(err error) bool {
	return errors.Is(err, ErrState)
}

func IsVMError(err error) bool {
	return errors.Is(err, ErrVM)
}

func IsCompilationFailedError(err error) bool {
	return errors.Is(err, ErrCompilationFailed)
}
