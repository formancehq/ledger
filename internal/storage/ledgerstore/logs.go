package ledgerstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	storageerrors "github.com/formancehq/ledger/internal/storage/sqlutils"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

const (
	LogTableName = "logs"
)

type Logs struct {
	bun.BaseModel `bun:"logs,alias:logs"`

	Ledger         string              `bun:"ledger,type:varchar"`
	ID             *bunpaginate.BigInt `bun:"id,unique,type:numeric"`
	Type           string              `bun:"type,type:log_type"`
	Hash           []byte              `bun:"hash,type:bytea"`
	Date           ledger.Time         `bun:"date,type:timestamptz"`
	Data           []byte              `bun:"data,type:jsonb"`
	IdempotencyKey string              `bun:"idempotency_key,type:varchar(256),unique"`
}

func (log *Logs) ToCore() *ledger.ChainedLog {

	payload, err := ledger.HydrateLog(ledger.LogTypeFromString(log.Type), log.Data)
	if err != nil {
		panic(errors.Wrap(err, "hydrating log data"))
	}

	return &ledger.ChainedLog{
		Log: ledger.Log{
			Type:           ledger.LogTypeFromString(log.Type),
			Data:           payload,
			Date:           log.Date.UTC(),
			IdempotencyKey: log.IdempotencyKey,
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
		selectQuery = selectQuery

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
	return store.withTransaction(ctx, func(tx bun.Tx) error {
		// Beware: COPY query is not supported by bun if the pgx driver is used.
		stmt, err := tx.Prepare(pq.CopyInSchema(
			store.bucket.name,
			LogTableName,
			"ledger", "id", "type", "hash", "date", "data", "idempotency_key",
		))
		if err != nil {
			return storageerrors.PostgresError(err)
		}

		ls := make([]Logs, len(activeLogs))
		for i, chainedLogs := range activeLogs {
			data, err := json.Marshal(chainedLogs.Data)
			if err != nil {
				return errors.Wrap(err, "marshaling log data")
			}

			ls[i] = Logs{
				Ledger:         store.name,
				ID:             (*bunpaginate.BigInt)(chainedLogs.ID),
				Type:           chainedLogs.Type.String(),
				Hash:           chainedLogs.Hash,
				Date:           chainedLogs.Date,
				Data:           data,
				IdempotencyKey: chainedLogs.IdempotencyKey,
			}

			_, err = stmt.Exec(ls[i].Ledger, ls[i].ID, ls[i].Type, ls[i].Hash, ls[i].Date, RawMessage(ls[i].Data), chainedLogs.IdempotencyKey)
			if err != nil {
				return storageerrors.PostgresError(err)
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			return storageerrors.PostgresError(err)
		}

		return stmt.Close()
	})
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

func (store *Store) GetLogs(ctx context.Context, q GetLogsQuery) (*api.Cursor[ledger.ChainedLog], error) {
	logs, err := paginateWithColumn[PaginatedQueryOptions[any], Logs](store, ctx,
		(*bunpaginate.ColumnPaginatedQuery[PaginatedQueryOptions[any]])(&q),
		store.logsQueryBuilder(q.Options),
	)
	if err != nil {
		return nil, err
	}

	return api.MapCursor(logs, func(from Logs) ledger.ChainedLog {
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

func NewGetLogsQuery(options PaginatedQueryOptions[any]) GetLogsQuery {
	return GetLogsQuery{
		PageSize: options.PageSize,
		Column:   "id",
		Order:    bunpaginate.OrderDesc,
		Options:  options,
	}
}
