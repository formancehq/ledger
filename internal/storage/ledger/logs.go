package ledger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Log struct {
	bun.BaseModel `bun:"table:logs,alias:logs"`

	Ledger         string     `bun:"ledger,type:varchar"`
	ID             int        `bun:"id,unique,type:numeric"`
	Type           string     `bun:"type,type:log_type"`
	Hash           []byte     `bun:"hash,type:bytea,scanonly"`
	Date           time.Time  `bun:"date,type:timestamptz"`
	Data           RawMessage `bun:"data,type:jsonb"`
	IdempotencyKey *string    `bun:"idempotency_key,type:varchar(256),unique"`
}

func (log Log) toCore() ledger.Log {

	payload, err := ledger.HydrateLog(ledger.LogTypeFromString(log.Type), log.Data)
	if err != nil {
		panic(errors.Wrap(err, "hydrating log data"))
	}

	return ledger.Log{
		Type: ledger.LogTypeFromString(log.Type),
		Data: payload,
		IdempotencyKey: func() string {
			if log.IdempotencyKey != nil {
				return *log.IdempotencyKey
			}
			return ""
		}(),
		Date: log.Date.UTC(),
		ID:   log.ID,
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

func (s *Store) InsertLog(ctx context.Context, log *ledger.Log) error {

	// we lock logs table as we need than the last log does not change until the transaction commit
	if s.ledger.HasFeature(ledger.FeatureHashLogs, "SYNC") {
		_, err := s.db.NewRaw(`select pg_advisory_xact_lock(hashtext(?))`, s.ledger.Name).Exec(ctx)
		if err != nil {
			return postgres.ResolveError(err)
		}
	}

	_, err := tracing.TraceWithLatency(ctx, "InsertLog", tracing.NoResult(func(ctx context.Context) error {

		data, err := json.Marshal(log.Data)
		if err != nil {
			return errors.Wrap(err, "failed to marshal log data")
		}

		newLog := &Log{
			Ledger: s.ledger.Name,
			Type:   log.Type.String(),
			Data:   data,
			Date:   log.Date,
			IdempotencyKey: func() *string {
				if log.IdempotencyKey == "" {
					return nil
				}
				return &log.IdempotencyKey
			}(),
		}

		_, err = s.db.
			NewInsert().
			Model(newLog).
			ModelTableExpr(s.GetPrefixedRelationName("logs")).
			Value("id", "nextval(?)", s.GetPrefixedRelationName(fmt.Sprintf(`"log_id_%d"`, s.ledger.ID))).
			Returning("*").
			Exec(ctx)
		if err != nil {
			err := postgres.ResolveError(err)
			switch {
			case errors.Is(err, postgres.ErrConstraintsFailed{}):
				if err.(postgres.ErrConstraintsFailed).GetConstraint() == "logs_idempotency_key" {
					return ledgercontroller.NewErrIdempotencyKeyConflict(log.IdempotencyKey)
				}
			default:
				return errors.Wrap(err, "inserting log")
			}
		}

		log.ID = newLog.ID
		log.Hash = newLog.Hash

		return nil
	}))

	return err
}

func (s *Store) ListLogs(ctx context.Context, q ledgercontroller.GetLogsQuery) (*bunpaginate.Cursor[ledger.Log], error) {
	return tracing.TraceWithLatency(ctx, "ListLogs", func(ctx context.Context) (*bunpaginate.Cursor[ledger.Log], error) {
		selectQuery := s.db.NewSelect().
			ModelTableExpr(s.GetPrefixedRelationName("logs")).
			ColumnExpr("*").
			Where("ledger = ?", s.ledger.Name)

		if q.Options.QueryBuilder != nil {
			subQuery, args, err := q.Options.QueryBuilder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
				switch {
				case key == "date":
					return fmt.Sprintf("%s %s ?", key, query.DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
				default:
					return "", nil, fmt.Errorf("unknown key '%s' when building query", key)
				}
			}))
			if err != nil {
				return nil, err
			}
			selectQuery = selectQuery.Where(subQuery, args...)
		}

		cursor, err := bunpaginate.UsingColumn[ledgercontroller.PaginatedQueryOptions[any], Log](ctx, selectQuery, bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[any]](q))
		if err != nil {
			return nil, err
		}

		return bunpaginate.MapCursor(cursor, Log.toCore), nil
	})
}

func (s *Store) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*ledger.Log, error) {
	return tracing.TraceWithLatency(ctx, "ReadLogWithIdempotencyKey", func(ctx context.Context) (*ledger.Log, error) {
		ret := &Log{}
		if err := s.db.NewSelect().
			Model(ret).
			ModelTableExpr(s.GetPrefixedRelationName("logs")).
			Column("*").
			Where("idempotency_key = ?", key).
			Where("ledger = ?", s.ledger.Name).
			Limit(1).
			Scan(ctx); err != nil {
			return nil, postgres.ResolveError(err)
		}

		return pointer.For(ret.toCore()), nil
	})
}
