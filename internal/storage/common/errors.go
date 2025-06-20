package common

import "fmt"

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

type ErrNotPaginatedField struct {
	field string
}

func (e ErrNotPaginatedField) Error() string {
	return fmt.Sprintf("field %s is not paginated", e.field)
}

func (e ErrNotPaginatedField) Is(err error) bool {
	_, ok := err.(ErrNotPaginatedField)
	return ok
}

func newErrNotPaginatedField(field string) ErrNotPaginatedField {
	return ErrNotPaginatedField{
		field: field,
	}
}