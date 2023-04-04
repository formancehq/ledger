package pagination

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/uptrace/bun"
)

type columnScanner[T any] func(t *T, scanner interface {
	Scan(args ...any) error
}) (uint64, error)

func UsingColumn[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query storage.ColumnPaginatedQuery[Q], fn columnScanner[T]) (*api.Cursor[T], error) {
	ret := make([]T, 0)

	sb = sb.Limit(int(query.PageSize) + 1) // Fetch one additional item to find the next token
	order := query.Order
	if query.Reverse {
		order = (order + 1) % 2
	}
	sb = sb.OrderExpr(fmt.Sprintf("%s %s", query.Column, order))

	if query.PaginationID != nil {
		if query.Reverse {
			switch query.Order {
			case storage.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s < ?", query.Column), query.PaginationID)
			case storage.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s > ?", query.Column), query.PaginationID)
			}
		} else {
			switch query.Order {
			case storage.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s >= ?", query.Column), query.PaginationID)
			case storage.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s <= ?", query.Column), query.PaginationID)
			}
		}
	}

	rows, err := sb.Rows(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	var (
		paginationIDs = make([]uint64, 0)
	)
	for rows.Next() {
		var t T
		paginationID, err := fn(&t, rows)
		if err != nil {
			return nil, err
		}
		if query.Bottom == nil {
			query.Bottom = &paginationID
		}
		paginationIDs = append(paginationIDs, paginationID)
		ret = append(ret, t)
	}
	if rows.Err() != nil {
		return nil, storageerrors.PostgresError(err)
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

	var previous, next *storage.ColumnPaginatedQuery[Q]

	if query.Reverse {
		cp := query
		cp.Reverse = false
		next = &cp

		if hasMore {
			cp := query
			cp.PaginationID = ptr(paginationIDs[len(paginationIDs)-2])
			previous = &cp
		}
	} else {
		if hasMore {
			cp := query
			cp.PaginationID = ptr(paginationIDs[len(paginationIDs)-1])
			next = &cp
		}
		if query.PaginationID != nil {
			if (query.Order == storage.OrderAsc && *query.PaginationID > *query.Bottom) || (query.Order == storage.OrderDesc && *query.PaginationID < *query.Bottom) {
				cp := query
				cp.Reverse = true
				previous = &cp
			}
		}
	}

	return &api.Cursor[T]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}
