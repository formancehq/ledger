package common

import (
	"database/sql"
	"fmt"
)

var ErrNotFound = sql.ErrNoRows

type ErrInvalidQuery struct {
	msg string
}

func (e ErrInvalidQuery) Error() string {
	return e.msg
}

func (e ErrInvalidQuery) Is(err error) bool {
	_, ok := err.(ErrInvalidQuery)
	return ok
}

func NewErrInvalidQuery(msg string, args ...any) ErrInvalidQuery {
	return ErrInvalidQuery{
		msg: fmt.Sprintf(msg, args...),
	}
}
