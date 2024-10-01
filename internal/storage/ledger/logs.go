package ledger

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/tracing"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/pkg/errors"
)

// Log override ledger.Log to be able to properly read/write payload which is jsonb
// on the database and 'any' on the Log structure (data column)
type Log struct {
	*ledger.Log `bun:",extend"`

	Ledger string     `bun:"ledger,type:varchar"`
	Data   RawMessage `bun:"data,type:jsonb"`
}

func (log Log) toCore() ledger.Log {
	payload, err := ledger.HydrateLog(log.Type, log.Data)
	if err != nil {
		panic(errors.Wrap(err, "hydrating log data"))
	}
	log.Log.Data = payload

	return *log.Log
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (s *Store) InsertLog(ctx context.Context, log *ledger.Log) error {

	// We lock logs table as we need than the last log does not change until the transaction commit
	if s.ledger.HasFeature(ledger.FeatureHashLogs, "SYNC") {
		_, err := s.db.NewRaw(`select pg_advisory_xact_lock(hashtext(?))`, s.ledger.Name).Exec(ctx)
		if err != nil {
			return err
		}
		lastLog := &ledger.Log{}
		err = s.db.NewSelect().
			Model(lastLog).
			ModelTableExpr(s.GetPrefixedRelationName("logs")).
			Order("seq desc").
			Where("ledger = ?", s.ledger.Name).
			Limit(1).
			Scan(ctx)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return errors.Wrap(err, "retrieving last log")
			}
			log.ComputeHash(nil)
		} else {
			log.ComputeHash(lastLog)
		}
	}

	_, err := tracing.TraceWithLatency(ctx, "InsertLog", tracing.NoResult(func(ctx context.Context) error {
		data, err := json.Marshal(log.Data)
		if err != nil {
			return errors.Wrap(err, "failed to marshal log data")
		}

		_, err = s.db.
			NewInsert().
			Model(&Log{
				Log:    log,
				Ledger: s.ledger.Name,
				Data:   data,
			}).
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
