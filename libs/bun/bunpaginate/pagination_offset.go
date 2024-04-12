package bunpaginate

import (
	"context"

	"github.com/uptrace/bun"
)

func usingOffset[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query OffsetPaginatedQuery[Q], withModel bool, builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*Cursor[T], error) {

	ret := make([]T, 0)

	if withModel {
		sb = sb.Model(&ret)
	}

	for _, builder := range builders {
		sb = sb.Apply(builder)
	}

	if query.Offset > 0 {
		sb = sb.Offset(int(query.Offset))
	}

	if query.PageSize > 0 {
		sb = sb.Limit(int(query.PageSize) + 1)
	}

	if withModel {
		if err := sb.Scan(ctx); err != nil {
			return nil, err
		}
	} else {
		if err := sb.Scan(ctx, &ret); err != nil {
			return nil, err
		}
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
	if query.PageSize != 0 && len(ret) > int(query.PageSize) {
		cp := query
		cp.Offset = query.Offset + query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	return &Cursor[T]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}

func UsingOffset[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query OffsetPaginatedQuery[Q], builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*Cursor[T], error) {

	return usingOffset[Q, T](ctx, sb, query, true, builders...)

}

func UsingOffsetWithoutModel[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query OffsetPaginatedQuery[Q], builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*Cursor[T], error) {

	return usingOffset[Q, T](ctx, sb, query, false, builders...)

}
