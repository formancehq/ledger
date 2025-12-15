package ledger

import (
	"errors"
	"fmt"
)

var (
	ErrNoLeader = errors.New("no leader")
)

type NotFoundError struct{
	msg string
}

func (e *NotFoundError) Error() string {
	return e.msg
}

func (e *NotFoundError) Is(err error) bool {
	_, ok := err.(*NotFoundError)
	return ok
}

func NewNotFoundError(f string, args ...any) *NotFoundError {
	return &NotFoundError{msg: fmt.Sprintf(f, args...)}
}
