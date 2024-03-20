package bunpaginate

import "github.com/formancehq/stack/libs/go-libs/collectionutils"

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
