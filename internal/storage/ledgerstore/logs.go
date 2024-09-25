package ledgerstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/v2/internal"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Logs struct {
	bun.BaseModel `bun:"table:logs,alias:logs"`

	Ledger         string              `bun:"ledger,type:varchar"`
	ID             *bunpaginate.BigInt `bun:"id,unique,type:numeric"`
	Type           string              `bun:"type,type:log_type"`
	Hash           []byte              `bun:"hash,type:bytea"`
	Date           time.Time           `bun:"date,type:timestamptz"`
	Data           RawMessage          `bun:"data,type:jsonb"`
	IdempotencyKey *string             `bun:"idempotency_key,type:varchar(256),unique"`
}

func (log *Logs) ToCore() *ledger.ChainedLog {

	payload, err := ledger.HydrateLog(ledger.LogTypeFromString(log.Type), log.Data)
	if err != nil {
		panic(errors.Wrap(err, "hydrating log data"))
	}

	return &ledger.ChainedLog{
		Log: ledger.Log{
			Type: ledger.LogTypeFromString(log.Type),
			Data: payload,
			Date: log.Date.UTC(),
			IdempotencyKey: func() string {
				if log.IdempotencyKey != nil {
					return *log.IdempotencyKey
				}
				return ""
			}(),
		},
		ID:   (*big.Int)(log.ID),
		Hash: log.Hash,
	}
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (store *Store) logsQueryBuilder(q PaginatedQueryOptions[any]) func(*bun.SelectQuery) *bun.SelectQuery {
	return func(selectQuery *bun.SelectQuery) *bun.SelectQuery {

		selectQuery = selectQuery.Where("ledger = ?", store.name)
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

func (store *Store) InsertLogs(ctx context.Context, activeLogs ...*ledger.ChainedLog) error {
	_, err := store.bucket.db.
		NewInsert().
		Model(pointer.For(collectionutils.Map(activeLogs, func(from *ledger.ChainedLog) Logs {
			data, err := json.Marshal(from.Data)
			if err != nil {
				panic(err)
			}

			return Logs{
				Ledger: store.name,
				ID:     (*bunpaginate.BigInt)(from.ID),
				Type:   from.Type.String(),
				Hash:   from.Hash,
				Date:   from.Date,
				Data:   data,
				IdempotencyKey: func() *string {
					if from.IdempotencyKey != "" {
						return &from.IdempotencyKey
					}
					return nil
				}(),
			}
		}))).
		Exec(ctx)
	return err
}

func (store *Store) GetLastLog(ctx context.Context) (*ledger.ChainedLog, error) {
	ret, err := fetch[*Logs](store, true, ctx,
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return query.
				OrderExpr("id desc").
				Where("ledger = ?", store.name).
				Limit(1)
		})
	if err != nil {
		return nil, err
	}

	return ret.ToCore(), nil
}

func (store *Store) GetLogs(ctx context.Context, q GetLogsQuery) (*bunpaginate.Cursor[ledger.ChainedLog], error) {
	logs, err := paginateWithColumn[PaginatedQueryOptions[any], Logs](store, ctx,
		(*bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[any]])(&q),
		store.logsQueryBuilder(q.Options),
	)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(logs, func(from Logs) ledger.ChainedLog {
		return *from.ToCore()
	}), nil
}

func (store *Store) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*ledger.ChainedLog, error) {
	ret, err := fetch[*Logs](store, true, ctx,
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return query.
				OrderExpr("id desc").
				Limit(1).
				Where("idempotency_key = ?", key).
				Where("ledger = ?", store.name)
		})
	if err != nil {
		return nil, err
	}

	return ret.ToCore(), nil
}

type GetLogsQuery bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[any]]

func (q GetLogsQuery) WithOrder(order bunpaginate.Order) GetLogsQuery {
	q.Order = order
	return q
}

func NewGetLogsQuery(options PaginatedQueryOptions[any]) GetLogsQuery {
	return GetLogsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}
