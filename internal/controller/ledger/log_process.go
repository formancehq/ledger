package ledger

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/pointer"

	ledger "github.com/formancehq/ledger/internal"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type logProcessor[INPUT any, OUTPUT ledger.LogPayload] struct {
	deadLockCounter metric.Int64Counter
	operation       string
}

func newLogProcessor[INPUT any, OUTPUT ledger.LogPayload](operation string, deadlockCounter metric.Int64Counter) *logProcessor[INPUT, OUTPUT] {
	return &logProcessor[INPUT, OUTPUT]{
		operation:       operation,
		deadLockCounter: deadlockCounter,
	}
}

func (lp *logProcessor[INPUT, OUTPUT]) runTx(
	ctx context.Context,
	store Store,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, sqlTX Store, schema *ledger.Schema, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*ledger.Log, *OUTPUT, error) {
	store, _, err := store.BeginTX(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	log, output, err := lp.runLog(ctx, store, parameters, fn)
	if err != nil {
		if rollbackErr := store.Rollback(ctx); rollbackErr != nil {
			logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
		}
		return nil, nil, err
	}

	if parameters.DryRun {
		if rollbackErr := store.Rollback(ctx); rollbackErr != nil {
			logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
		}
		return log, output, nil
	}

	if err := store.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return log, output, nil
}

func (lp *logProcessor[INPUT, OUTPUT]) runLog(
	ctx context.Context,
	store Store,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, sqlTX Store, schema *ledger.Schema, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*ledger.Log, *OUTPUT, error) {

	var schema *ledger.Schema
	if parameters.SchemaVersion != "" {
		var err error
		schema, err = store.FindSchema(ctx, parameters.SchemaVersion)
		if err != nil {
			if errors.Is(err, postgres.ErrNotFound) {
				latestVersion, err := store.FindLatestSchemaVersion(ctx)
				if err != nil {
					return nil, nil, err
				}
				return nil, nil, ErrSchemaNotFound{
					requestedVersion: parameters.SchemaVersion,
					latestVersion:    latestVersion,
				}
			}
			return nil, nil, err
		}
	} else {
		var payload OUTPUT
		if payload.NeedsSchema() {
			// Only allow a missing schema validation if the ledger doesn't have one
			latestVersion, err := store.FindLatestSchemaVersion(ctx)
			if err != nil {
				return nil, nil, err
			}
			if latestVersion != nil {
				return nil, nil, ErrSchemaNotSpecified{
					latestVersion: *latestVersion,
				}
			}
		}
	}

	output, err := fn(ctx, store, schema, parameters)
	if err != nil {
		return nil, nil, err
	}
	log := ledger.NewLog(*output)
	log.IdempotencyKey = parameters.IdempotencyKey
	log.IdempotencyHash = ledger.ComputeIdempotencyHash(parameters.Input)
	log.SchemaVersion = parameters.SchemaVersion

	if schema != nil {
		if err := log.ValidateWithSchema(*schema); err != nil {
			return nil, nil, newErrSchemaValidationError(parameters.SchemaVersion, err)
		}
	}

	err = store.InsertLog(ctx, &log)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert log: %w", err)
	}
	logging.FromContext(ctx).Debugf("log inserted with id %d", *log.ID)

	return &log, output, err
}

func (lp *logProcessor[INPUT, OUTPUT]) forgeLog(
	ctx context.Context,
	store Store,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, store Store, schema *ledger.Schema, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*ledger.Log, *OUTPUT, bool, error) {
	if parameters.IdempotencyKey != "" {
		log, output, err := lp.fetchLogWithIK(ctx, store, parameters)
		if err != nil {
			return nil, nil, false, err
		}
		if output != nil {
			return log, output, true, nil
		}
	}

	for {
		log, output, err := lp.runTx(ctx, store, parameters, fn)
		if err != nil {
			switch {
			case errors.Is(err, postgres.ErrDeadlockDetected):
				trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("deadlock", true))
				logging.FromContext(ctx).Info("deadlock detected, retrying...")
				lp.deadLockCounter.Add(ctx, 1, metric.WithAttributes(
					attribute.String("operation", lp.operation),
				))
				continue
			// A log with the IK could have been inserted in the meantime, read again the database to retrieve it
			case errors.Is(err, ledgerstore.ErrIdempotencyKeyConflict{}):
				log, output, err := lp.fetchLogWithIK(ctx, store, parameters)
				if err != nil {
					return nil, nil, false, err
				}
				if output == nil {
					panic("incoherent error, received duplicate IK but log not found in database")
				}

				return log, output, true, nil
			default:
				return nil, nil, false, fmt.Errorf("unexpected error while forging log: %w", err)
			}
		}

		return log, output, false, nil
	}
}

func (lp *logProcessor[INPUT, OUTPUT]) fetchLogWithIK(ctx context.Context, store Store, parameters Parameters[INPUT]) (*ledger.Log, *OUTPUT, error) {
	log, err := store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
	if err != nil && !errors.Is(err, postgres.ErrNotFound) {
		return nil, nil, err
	}
	if err == nil {
		// notes(gfyrag): idempotency hash should never be empty in this case, but data from previous
		// ledger version does not have this field and it cannot be recomputed
		if len(log.IdempotencyHash) > 0 {
			if computedHash := ledger.ComputeIdempotencyHash(parameters.Input); log.IdempotencyHash != computedHash {
				return nil, nil, newErrInvalidIdempotencyInputs(log.IdempotencyKey, log.IdempotencyHash, computedHash)
			}
		}

		return log, pointer.For(log.Data.(OUTPUT)), nil
	}
	return nil, nil, nil
}
