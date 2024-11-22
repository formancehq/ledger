package ledger

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
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
	fn func(ctx context.Context, sqlTX Store, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*ledger.Log, *OUTPUT, error) {
	var (
		output *OUTPUT
		log    ledger.Log
	)
	if err := store.BeginTX(ctx, nil); err != nil {
		return nil, nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		_ = store.Rollback()
	}()

	output, err := fn(ctx, store, parameters)
	if err != nil {
		return nil, nil, err
	}
	log = ledger.NewLog(*output)
	log.IdempotencyKey = parameters.IdempotencyKey
	log.IdempotencyHash = ledger.ComputeIdempotencyHash(parameters.Input)

	err = store.InsertLog(ctx, &log)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to insert log: %w", err)
	}
	logging.FromContext(ctx).Debugf("log inserted with id %d", log.ID)

	if parameters.DryRun {
		return &log, output, nil
	}

	if err := store.Commit(); err != nil {
		return nil, nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &log, output, err
}

func (lp *logProcessor[INPUT, OUTPUT]) forgeLog(
	ctx context.Context,
	store Store,
	parameters Parameters[INPUT],
	fn func(ctx context.Context, store Store, parameters Parameters[INPUT]) (*OUTPUT, error),
) (*ledger.Log, *OUTPUT, error) {
	if parameters.IdempotencyKey != "" {
		log, output, err := lp.fetchLogWithIK(ctx, store, parameters)
		if err != nil {
			return nil, nil, err
		}
		if output != nil {
			return log, output, nil
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
			case errors.Is(err, ErrIdempotencyKeyConflict{}):
				log, output, err := lp.fetchLogWithIK(ctx, store, parameters)
				if err != nil {
					return nil, nil, err
				}
				if output == nil {
					panic("incoherent error, received duplicate IK but log not found in database")
				}

				return log, output, nil
			default:
				return nil, nil, fmt.Errorf("unexpected error while forging log: %w", err)
			}
		}

		return log, output, nil
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
		if log.IdempotencyHash != "" {
			if computedHash := ledger.ComputeIdempotencyHash(parameters.Input); log.IdempotencyHash != computedHash {
				return nil, nil, newErrInvalidIdempotencyInputs(log.IdempotencyKey, log.IdempotencyHash, computedHash)
			}
		}

		return log, pointer.For(log.Data.(OUTPUT)), nil
	}
	return nil, nil, nil
}
