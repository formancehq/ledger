package ledgerstore

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"

	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

func (store *Store) logsQueryBuilder(q ledgercontroller.PaginatedQueryOptions[any]) func(*bun.SelectQuery) *bun.SelectQuery {
	return func(selectQuery *bun.SelectQuery) *bun.SelectQuery {

		selectQuery = selectQuery.Where("ledger = ?", store.name).ModelTableExpr(store.GetPrefixedRelationName("logs"))
		if q.QueryBuilder != nil {
			subQuery, args, err := q.QueryBuilder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
				switch {
				case key == "date":
					return fmt.Sprintf("%s %s ?", key, query.DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
				default:
					return "", nil, fmt.Errorf("unknown key '%s' when building query", key)
				}
			}))
			if err != nil {
				panic(err)
			}
			selectQuery = selectQuery.Where(subQuery, args...)
		}

		return selectQuery
	}
}

func (store *Store) GetLogs(ctx context.Context, q ledgercontroller.GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	logs, err := paginateWithColumn[ledgercontroller.PaginatedQueryOptions[any], ledgerstore.Log](store, ctx,
		(*bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[any]])(&q),
		store.logsQueryBuilder(q.Options),
	)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(logs, func(from ledgerstore.Log) ledger.Log {
		return from.ToCore()
	}), nil
}
