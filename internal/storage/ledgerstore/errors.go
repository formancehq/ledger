package ledgerstore

import (
	"fmt"

	"github.com/pkg/errors"
)

type errInvalidQuery struct {
	msg string
}

func (e *errInvalidQuery) Error() string {
	return e.msg
}

func (e *errInvalidQuery) Is(err error) bool {
	_, ok := err.(*errInvalidQuery)
	return ok
}

func newErrInvalidQuery(msg string, args ...any) *errInvalidQuery {
	return &errInvalidQuery{
		msg: fmt.Sprintf(msg, args...),
	}
}

func IsErrInvalidQuery(err error) bool {
	return errors.Is(err, &errInvalidQuery{})
}
