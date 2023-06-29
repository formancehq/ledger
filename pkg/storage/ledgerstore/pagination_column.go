package ledgerstore

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	storageerrors "github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/uptrace/bun"
)

func UsingColumn[FILTERS any, ENTITY any](ctx context.Context,
	builder func(filters FILTERS, models *[]ENTITY) *bun.SelectQuery,
	query ColumnPaginatedQuery[FILTERS]) (*api.Cursor[ENTITY], error) {
	ret := make([]ENTITY, 0)

	sb := builder(query.Filters, &ret)
	sb = sb.Limit(int(query.PageSize) + 1) // Fetch one additional item to find the next token
	order := query.Order
	if query.Reverse {
		order = order.Reverse()
	}
	sb = sb.OrderExpr(fmt.Sprintf("%s %s", query.Column, order))

	if query.PaginationID != nil {
		if query.Reverse {
			switch query.Order {
			case OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s < ?", query.Column), query.PaginationID)
			case OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s > ?", query.Column), query.PaginationID)
			}
		} else {
			switch query.Order {
			case OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s >= ?", query.Column), query.PaginationID)
			case OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s <= ?", query.Column), query.PaginationID)
			}
		}
	}

	if err := sb.Scan(ctx); err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	var (
		paginatedColumnIndex = 0
	)
	typeOfT := reflect.TypeOf(ret).Elem()
	for ; paginatedColumnIndex < typeOfT.NumField(); paginatedColumnIndex++ {
		field := typeOfT.Field(paginatedColumnIndex)
		tag := field.Tag.Get("bun")
		column := strings.Split(tag, ",")[0]
		if column == query.Column {
			break
		}
	}

	var (
		paginationIDs = make([]uint64, 0)
	)
	for _, t := range ret {
		paginationID := reflect.ValueOf(t).
			Field(paginatedColumnIndex).
			Interface().(uint64)
		if query.Bottom == nil {
			query.Bottom = &paginationID
		}
		paginationIDs = append(paginationIDs, paginationID)
	}

	hasMore := len(ret) > int(query.PageSize)
	if hasMore {
		ret = ret[:len(ret)-1]
	}
	if query.Reverse {
		for i := 0; i < len(ret)/2; i++ {
			ret[i], ret[len(ret)-i-1] = ret[len(ret)-i-1], ret[i]
		}
	}

	var previous, next *ColumnPaginatedQuery[FILTERS]

	if query.Reverse {
		cp := query
		cp.Reverse = false
		next = &cp

		if hasMore {
			cp := query
			cp.PaginationID = pointer.For(paginationIDs[len(paginationIDs)-2])
			previous = &cp
		}
	} else {
		if hasMore {
			cp := query
			cp.PaginationID = pointer.For(paginationIDs[len(paginationIDs)-1])
			next = &cp
		}
		if query.PaginationID != nil {
			if (query.Order == OrderAsc && *query.PaginationID > *query.Bottom) || (query.Order == OrderDesc && *query.PaginationID < *query.Bottom) {
				cp := query
				cp.Reverse = true
				previous = &cp
			}
		}
	}

	return &api.Cursor[ENTITY]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}
