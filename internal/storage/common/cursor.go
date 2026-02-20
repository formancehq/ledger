package common

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
)

// todo: backport in go libs
func Extract[OF any](
	r *http.Request,
	defaulter func() (*InitialPaginatedQuery[OF], error),
	modifiers ...func(query *InitialPaginatedQuery[OF]) error,
) (PaginatedQuery[OF], error) {
	if r.URL.Query().Get(bunpaginate.QueryKeyCursor) != "" {
		return UnmarshalCursor[OF](r.URL.Query().Get(bunpaginate.QueryKeyCursor), modifiers...)
	} else {
		initialQuery, err := defaulter()
		if err != nil {
			return nil, fmt.Errorf("extracting paginated query: %w", err)
		}
		return *initialQuery, nil
	}
}

func UnmarshalCursor[Options any](v string, modifiers ...func(query *InitialPaginatedQuery[Options]) error) (PaginatedQuery[Options], error) {
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

	var q PaginatedQuery[Options]
	if x.Offset != nil { // Offset defined, this is an offset cursor
		q = &OffsetPaginatedQuery[Options]{}
	} else {
		q = &ColumnPaginatedQuery[Options]{}
	}

	if err := json.Unmarshal(res, &q); err != nil {
		return nil, err
	}

	var root *InitialPaginatedQuery[Options]
	if x.Offset != nil { // Offset defined, this is an offset cursor
		root = &q.(*OffsetPaginatedQuery[Options]).InitialPaginatedQuery
	} else {
		root = &q.(*ColumnPaginatedQuery[Options]).InitialPaginatedQuery
	}

	for _, modifier := range modifiers {
		if err := modifier(root); err != nil {
			return nil, err
		}
	}

	return reflect.ValueOf(q).Elem().Interface().(PaginatedQuery[Options]), nil
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

		query, err = UnmarshalCursor[Options](cursor.Next)
		if err != nil {
			return fmt.Errorf("paginating next request: %w", err)
		}
	}

	return nil
}

func encodeCursor[OptionsType any, PaginatedQueryType PaginatedQuery[OptionsType]](v *PaginatedQueryType) string {
	if v == nil {
		return ""
	}
	return bunpaginate.EncodeCursor(v)
}
