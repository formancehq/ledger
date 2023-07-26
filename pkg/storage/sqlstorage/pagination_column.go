package sqlstorage

import (
	"context"
	"fmt"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/ledger"
)

type Scanner interface {
	Scan(args ...any) error
}
type entityFactory[ENTITY any] func(scanner Scanner) (ENTITY, uint64, error)

func ptr[V any](v V) *V {
	return &v
}

func UsingColumn[FILTERS any, ENTITY any](ctx context.Context,
	executor executor,
	builder func(filters FILTERS, models *[]ENTITY) *sqlbuilder.SelectBuilder,
	query ledger.ColumnPaginatedQuery[FILTERS],
	entityFactory entityFactory[ENTITY],
	flavor Flavor,
) (*sharedapi.Cursor[ENTITY], error) {
	ret := make([]ENTITY, 0)

	sb := builder(query.Filters, &ret)
	sb = sb.Limit(int(query.PageSize) + 1) // Fetch one additional item to find the next token
	order := query.Order
	if query.Reverse {
		order = order.Reverse()
	}
	sb = sb.OrderBy(fmt.Sprintf("%s %s", query.Column, order))

	if query.PaginationID != nil {
		if query.Reverse {
			switch query.Order {
			case ledger.OrderAsc:
				sb = sb.Where(sb.L(query.Column, query.PaginationID))
			case ledger.OrderDesc:
				sb = sb.Where(sb.G(query.Column, query.PaginationID))
			}
		} else {
			switch query.Order {
			case ledger.OrderAsc:
				sb = sb.Where(sb.GE(query.Column, query.PaginationID))
			case ledger.OrderDesc:
				sb = sb.Where(sb.LE(query.Column, query.PaginationID))
			}
		}
	}

	sql, args := sb.BuildWithFlavor(sqlbuilder.Flavor(flavor))

	rows, err := executor.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		paginationIDs = make([]uint64, 0)
	)
	for rows.Next() {
		entity, paginationID, err := entityFactory(rows)
		if err != nil {
			return nil, err
		}
		if query.Bottom == nil {
			query.Bottom = &paginationID
		}
		ret = append(ret, entity)
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

	var previous, next *ledger.ColumnPaginatedQuery[FILTERS]

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
			if (query.Order == ledger.OrderAsc && *query.PaginationID > *query.Bottom) || (query.Order == ledger.OrderDesc && *query.PaginationID < *query.Bottom) {
				cp := query
				cp.Reverse = true
				previous = &cp
			}
		}
	}

	return &sharedapi.Cursor[ENTITY]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}
