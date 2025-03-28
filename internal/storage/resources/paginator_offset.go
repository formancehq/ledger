package resources

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
	"math"
)

type OffsetPaginator[ResourceType, OptionsType any] struct {
	DefaultPaginationColumn string
	DefaultOrder            bunpaginate.Order
}

//nolint:unused
func (o OffsetPaginator[ResourceType, OptionsType]) Paginate(sb *bun.SelectQuery, query ledgercontroller.OffsetPaginatedQuery[OptionsType]) (*bun.SelectQuery, error) {

	paginationColumn := o.DefaultPaginationColumn
	originalOrder := o.DefaultOrder
	if query.Order != nil {
		originalOrder = *query.Order
	}

	orderExpression := fmt.Sprintf("%s %s", paginationColumn, originalOrder)
	sb = sb.ColumnExpr("row_number() OVER (ORDER BY " + orderExpression + ")")

	if query.Offset > math.MaxInt32 {
		return nil, fmt.Errorf("offset value exceeds maximum allowed value")
	}
	if query.Offset > 0 {
		sb = sb.Offset(int(query.Offset))
	}

	if query.PageSize > 0 {
		sb = sb.Limit(int(query.PageSize) + 1)
	}

	return sb, nil
}

//nolint:unused
func (o OffsetPaginator[ResourceType, OptionsType]) BuildCursor(ret []ResourceType, query ledgercontroller.OffsetPaginatedQuery[OptionsType]) (*bunpaginate.Cursor[ResourceType], error) {

	var previous, next *ledgercontroller.OffsetPaginatedQuery[OptionsType]

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
		// Check for potential overflow
		if query.Offset > math.MaxUint64-query.PageSize {
			return nil, fmt.Errorf("offset overflow")
		}
		cp.Offset = query.Offset + query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	return &bunpaginate.Cursor[ResourceType]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: encodeCursor[OptionsType, ledgercontroller.OffsetPaginatedQuery[OptionsType]](previous),
		Next:     encodeCursor[OptionsType, ledgercontroller.OffsetPaginatedQuery[OptionsType]](next),
		Data:     ret,
	}, nil
}

var _ Paginator[any, ledgercontroller.OffsetPaginatedQuery[any]] = &OffsetPaginator[any, any]{}
