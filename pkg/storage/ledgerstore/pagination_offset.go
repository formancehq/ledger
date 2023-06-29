package ledgerstore

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/uptrace/bun"
)

func UsingOffset[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query OffsetPaginatedQuery[Q]) (*api.Cursor[T], error) {
	ret := make([]T, 0)

	sb = sb.Offset(int(query.Offset))
	sb = sb.Limit(int(query.PageSize) + 1)

	if err := sb.Scan(ctx, &ret); err != nil {
		return nil, err
	}

	var previous, next *OffsetPaginatedQuery[Q]

	// Page with transactions before
	if query.Offset > 0 {
		cp := query
		offset := int(query.Offset) - int(query.PageSize)
		if offset < 0 {
			offset = 0
		}
		cp.Offset = uint64(offset)
		previous = &cp
	}

	// Page with transactions after
	if len(ret) > int(query.PageSize) {
		cp := query
		cp.Offset = query.Offset + query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	return &api.Cursor[T]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}
