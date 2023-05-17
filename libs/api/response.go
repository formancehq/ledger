package api

import (
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
)

type BaseResponse[T any] struct {
	Data   *T         `json:"data,omitempty"`
	Cursor *Cursor[T] `json:"cursor,omitempty"`
}

type Cursor[T any] struct {
	PageSize int    `json:"pageSize,omitempty"`
	HasMore  bool   `json:"hasMore"`
	Previous string `json:"previous,omitempty"`
	Next     string `json:"next,omitempty"`
	Data     []T    `json:"data"`
}

func MapCursor[FROM any, TO any](cursor *Cursor[FROM], mapper func(FROM) TO) *Cursor[TO] {
	return &Cursor[TO]{
		PageSize: cursor.PageSize,
		HasMore:  cursor.HasMore,
		Previous: cursor.Previous,
		Next:     cursor.Next,
		Data:     collectionutils.Map(cursor.Data, mapper),
	}
}

type ErrorResponse struct {
	ErrorCode    string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	Details      string `json:"details,omitempty"`
}
