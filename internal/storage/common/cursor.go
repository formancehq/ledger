package common

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"net/http"
	"reflect"
)

// todo: backport in go libs
func Extract[OF any](
	r *http.Request,
	defaulter func() (*InitialPaginatedQuery[OF], error),
	modifiers ...func(query *InitialPaginatedQuery[OF]) error,
) (PaginatedQuery[OF], error) {
	if r.URL.Query().Get(bunpaginate.QueryKeyCursor) != "" {
		return unmarshalCursor[OF](r.URL.Query().Get(bunpaginate.QueryKeyCursor), modifiers...)
	} else {
		initialQuery, err := defaulter()
		if err != nil {
			return nil, fmt.Errorf("extracting paginated query: %w", err)
		}
		return *initialQuery, nil
	}
}

func unmarshalCursor[OF any](v string, modifiers ...func(query *InitialPaginatedQuery[OF]) error) (PaginatedQuery[OF], error) {
	res, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return nil, err
	}

	// todo: we should better rely on schema to determine the type of cursor
	type aux struct {
		Offset *uint64 `json:"offset"`
	}
	x := aux{}
	if err := json.Unmarshal(res, &x); err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}

	var q PaginatedQuery[OF]
	if x.Offset != nil { // Offset defined, this is an offset cursor
		q = &OffsetPaginatedQuery[OF]{}
	} else {
		q = &ColumnPaginatedQuery[OF]{}
	}

	if err := json.Unmarshal(res, &q); err != nil {
		return nil, err
	}

	var root *InitialPaginatedQuery[OF]
	if x.Offset != nil { // Offset defined, this is an offset cursor
		root = &q.(*OffsetPaginatedQuery[OF]).InitialPaginatedQuery
	} else {
		root = &q.(*ColumnPaginatedQuery[OF]).InitialPaginatedQuery
	}

	for _, modifier := range modifiers {
		if err := modifier(root); err != nil {
			return nil, err
		}
	}

	return reflect.ValueOf(q).Elem().Interface().(PaginatedQuery[OF]), nil
}

func Iterate[OF any, Options any](
	ctx context.Context,
	initialQuery InitialPaginatedQuery[Options],
	iterator func(ctx context.Context, q PaginatedQuery[Options]) (*bunpaginate.Cursor[OF], error),
	cb func(cursor *bunpaginate.Cursor[OF]) error,
) error {

	var query PaginatedQuery[OF] = initialQuery
	for {
		cursor, err := iterator(ctx, query)
		if err != nil {
			return err
		}

		if err := cb(cursor); err != nil {
			return err
		}

		if !cursor.HasMore {
			break
		}

		query, err = unmarshalCursor[OF](cursor.Next)
		if err != nil {
			return fmt.Errorf("paginating next request: %w", err)
		}
	}

	return nil
}
