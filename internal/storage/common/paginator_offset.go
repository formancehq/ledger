package common

import (
	"fmt"
	"math"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
)

type OffsetPaginator[ResourceType, OptionsType any] struct {
	query OffsetPaginatedQuery[OptionsType]
}

//nolint:unused
func (o OffsetPaginator[ResourceType, OptionsType]) Paginate(sb *bun.SelectQuery) (*bun.SelectQuery, error) {

	paginationColumn := o.query.Column
	originalOrder := *o.query.Order

	orderExpression := fmt.Sprintf("%s %s", paginationColumn, originalOrder)
	sb = sb.ColumnExpr("row_number() OVER (ORDER BY " + orderExpression + ")")

	if o.query.Offset > math.MaxInt32 {
		return nil, fmt.Errorf("offset value exceeds maximum allowed value")
	}
	if o.query.Offset > 0 {
		sb = sb.Offset(int(o.query.Offset))
	}
	if o.query.PageSize > 0 {
		sb = sb.Limit(int(o.query.PageSize) + 1)
	}

	return sb, nil
}

//nolint:unused
func (o OffsetPaginator[ResourceType, OptionsType]) BuildCursor(ret []ResourceType) (*bunpaginate.Cursor[ResourceType], error) {

	var previous, next *OffsetPaginatedQuery[OptionsType]

	// Page with transactions before
	if o.query.Offset > 0 {
		cp := o.query
		offset := int(o.query.Offset) - int(o.query.PageSize)
		if offset < 0 {
			offset = 0
		}
		cp.Offset = uint64(offset)
		previous = &cp
	}

	// Page with transactions after
	if o.query.PageSize != 0 && len(ret) > int(o.query.PageSize) {
		cp := o.query
		// Check for potential overflow
		if o.query.Offset > math.MaxUint64-o.query.PageSize {
			return nil, fmt.Errorf("offset overflow")
		}
		cp.Offset = o.query.Offset + o.query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	return &bunpaginate.Cursor[ResourceType]{
		PageSize: int(o.query.PageSize),
		HasMore:  next != nil,
		Previous: encodeCursor[OptionsType, OffsetPaginatedQuery[OptionsType]](previous),
		Next:     encodeCursor[OptionsType, OffsetPaginatedQuery[OptionsType]](next),
		Data:     ret,
	}, nil
}

var _ Paginator[any] = &OffsetPaginator[any, any]{}

func newOffsetPaginator[ResourceType, OptionsType any](
	query OffsetPaginatedQuery[OptionsType],
) OffsetPaginator[ResourceType, OptionsType] {
	return OffsetPaginator[ResourceType, OptionsType]{query: query}
}
