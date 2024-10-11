package ledger

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/platform/postgres"
	"github.com/formancehq/go-libs/pointer"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func runTx[INPUT any, OUTPUT ledger.LogPayload](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*OUTPUT, error)) (*OUTPUT, error) {
	var payload *OUTPUT
	err := store.WithTX(ctx, nil, func(tx TX) (commit bool, err error) {
		payload, err = fn(ctx, tx, parameters.Input)
		if err != nil {
			return false, err
		}
		log := ledger.NewLog(*payload)
		log.IdempotencyKey = parameters.IdempotencyKey
		log.IdempotencyHash = ledger.ComputeIdempotencyHash(parameters.Input)

		_, err = tracing.TraceWithLatency(ctx, "InsertLog", func(ctx context.Context) (*struct{}, error) {
			return nil, tx.InsertLog(ctx, &log)
		})
		if err != nil {
			return false, fmt.Errorf("failed to insert log: %w", err)
		}
		logging.FromContext(ctx).Debugf("log inserted with id %d", log.ID)

		if parameters.DryRun {
			return false, nil
		}

		return true, nil
	})
	return payload, err
}

// todo: metrics, add deadlocks
func forgeLog[INPUT any, OUTPUT ledger.LogPayload](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*OUTPUT, error)) (*OUTPUT, error) {
	if parameters.IdempotencyKey != "" {
		output, err := fetchLogWithIK[INPUT, OUTPUT](ctx, store, parameters)
		if err != nil {
			return nil, err
		}
		if output != nil {
			return output, nil
		}
	}

	for {
		output, err := runTx(ctx, store, parameters, fn)
		if err != nil {
			switch {
			case errors.Is(err, postgres.ErrDeadlockDetected):
				trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("deadlock", true))
				logging.FromContext(ctx).Info("deadlock detected, retrying...")
				continue
			// A log with the IK could have been inserted in the meantime, read again the database to retrieve it
			case errors.Is(err, ErrIdempotencyKeyConflict{}):
				output, err := fetchLogWithIK[INPUT, OUTPUT](ctx, store, parameters)
				if err != nil {
					return nil, err
				}
				if output == nil {
					panic("incoherent error, received duplicate IK but log not found in database")
				}

				return output, nil
			default:
				return nil, fmt.Errorf("unexpected error while forging log: %w", err)
			}
		}

		return output, nil
	}
}

func fetchLogWithIK[INPUT any, OUTPUT ledger.LogPayload](ctx context.Context, store Store, parameters Parameters[INPUT]) (*OUTPUT, error) {
	log, err := store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
	if err != nil && !errors.Is(err, postgres.ErrNotFound) {
		return nil, err
	}
	if err == nil {
		// notes(gfyrag): idempotency hash should never be empty in this case, but data from previous
		// ledger version does not have this field and it cannot be recomputed
		if log.IdempotencyHash != "" {
			if computedHash := ledger.ComputeIdempotencyHash(parameters.Input); log.IdempotencyHash != computedHash {
				return nil, newErrInvalidIdempotencyInputs(log.IdempotencyKey, log.IdempotencyHash, computedHash)
			}
		}

		return pointer.For(log.Data.(OUTPUT)), nil
	}
	return nil, nil
}
