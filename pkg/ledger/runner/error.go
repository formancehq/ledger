package runner

import (
	"github.com/pkg/errors"
)

type ErrNoPostings struct{}

func (e ErrNoPostings) Error() string {
	return "transaction has no postings"
}

func (v ErrNoPostings) Is(err error) bool {
	_, ok := err.(*ErrNoPostings)
	return ok
}

func newErrNoPostings() ErrNoPostings {
	return ErrNoPostings{}
}

func IsNoPostingsError(err error) bool {
	return errors.Is(err, &ErrNoPostings{})
}
