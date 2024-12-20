package ledger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/formancehq/ledger/pkg/features"

	"errors"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/pointer"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

// Log override ledger.Log to be able to properly read/write payload which is jsonb
// on the database and 'any' on the Log structure (data column)
type Log struct {
	*ledger.Log `bun:",extend"`

	Ledger  string     `bun:"ledger,type:varchar"`
	Data    RawMessage `bun:"data,type:jsonb"`
	Memento RawMessage `bun:"memento,type:bytea"`
}

func (log Log) ToCore() ledger.Log {
	payload, err := ledger.HydrateLog(log.Type, log.Data)
	if err != nil {
		panic(fmt.Errorf("hydrating log data: %w", err))
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

func (store *Store) InsertLog(ctx context.Context, log *ledger.Log) error {

	_, err := tracing.TraceWithMetric(
		ctx,
		"InsertLog",
		store.tracer,
		store.insertLogHistogram,
		tracing.NoResult(func(ctx context.Context) error {

			// We lock logs table as we need than the last log does not change until the transaction commit
			if store.ledger.HasFeature(features.FeatureHashLogs, "SYNC") {
				_, err := store.db.NewRaw(`select pg_advisory_xact_lock(?)`, store.ledger.ID).Exec(ctx)
				if err != nil {
					return postgres.ResolveError(err)
				}
			}

			payloadData, err := json.Marshal(log.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal log data: %w", err)
			}

			mementoObject := log.Data.(any)
			if memento, ok := mementoObject.(ledger.Memento); ok {
				mementoObject = memento.GetMemento()
			}

			mementoData, err := json.Marshal(mementoObject)
			if err != nil {
				return err
			}

			query := store.db.
				NewInsert().
				Model(&Log{
					Log:     log,
					Ledger:  store.ledger.Name,
					Data:    payloadData,
					Memento: mementoData,
				}).
				ModelTableExpr(store.GetPrefixedRelationName("logs")).
				Returning("*")

			if log.ID == 0 {
				query = query.Value("id", "nextval(?)", store.GetPrefixedRelationName(fmt.Sprintf(`"log_id_%d"`, store.ledger.ID)))
			}

			_, err = query.Exec(ctx)
			if err != nil {
				err := postgres.ResolveError(err)
				switch {
				case errors.Is(err, postgres.ErrConstraintsFailed{}):
					if err.(postgres.ErrConstraintsFailed).GetConstraint() == "logs_idempotency_key" {
						return ledgercontroller.NewErrIdempotencyKeyConflict(log.IdempotencyKey)
					}
				default:
					return fmt.Errorf("inserting log: %w", err)
				}
			}

			return nil
		}),
	)

	return err
}

func (store *Store) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"ReadLogWithIdempotencyKey",
		store.tracer,
		store.readLogWithIdempotencyKeyHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			ret := &Log{}
			if err := store.db.NewSelect().
				Model(ret).
				ModelTableExpr(store.GetPrefixedRelationName("logs")).
				Column("*").
				Where("idempotency_key = ?", key).
				Where("ledger = ?", store.ledger.Name).
				Limit(1).
				Scan(ctx); err != nil {
				return nil, postgres.ResolveError(err)
			}

			return pointer.For(ret.ToCore()), nil
		},
	)
}
