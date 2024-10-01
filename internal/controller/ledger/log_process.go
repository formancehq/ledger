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
)

func runTx[INPUT, OUTPUT any](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*ledger.Log, *OUTPUT, error)) (*OUTPUT, error) {
	var (
		log    *ledger.Log
		output *OUTPUT
	)
	err := store.WithTX(ctx, nil, func(tx TX) (commit bool, err error) {
		log, output, err = fn(ctx, tx, parameters.Input)
		if err != nil {
			return false, err
		}
		log.IdempotencyKey = parameters.IdempotencyKey
		log.IdempotencyHash = ledger.ComputeIdempotencyHash(parameters.Input)

		_, err = tracing.TraceWithLatency(ctx, "InsertLog", func(ctx context.Context) (*struct{}, error) {
			return nil, tx.InsertLog(ctx, log)
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
	return output, err
}

// todo: handle too many clients error
// notes(gfyrag): how?
// By retrying? Is the server already overloaded? Add a limit on the retries number?
// Ask the client to retry later?
func forgeLog[INPUT, OUTPUT any](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*ledger.Log, *OUTPUT, error)) (*OUTPUT, error) {
	if parameters.IdempotencyKey != "" {
		log, err := store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, postgres.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			if computedHash := ledger.ComputeIdempotencyHash(parameters.Input); log.IdempotencyHash != computedHash {
				return nil, newErrInvalidIdempotencyInputs(log.IdempotencyKey, log.IdempotencyHash, computedHash)
			}

			return pointer.For(log.Data.(OUTPUT)), nil
		}
	}

	for {
		output, err := runTx(ctx, store, parameters, fn)
		if err != nil {
			switch {
			case errors.Is(err, postgres.ErrDeadlockDetected):
				logging.FromContext(ctx).Info("deadlock detected, retrying...")
				continue
			// A log with the IK could have been inserted in the meantime, read again the database to retrieve it
			case errors.Is(err, ErrIdempotencyKeyConflict{}):
				log, err := store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
				if err != nil && !errors.Is(err, postgres.ErrNotFound) {
					return nil, err
				}
				if errors.Is(err, postgres.ErrNotFound) {
					logging.FromContext(ctx).Errorf("incoherent error, received duplicate IK but log not found in database")
					return nil, err
				}

				return pointer.For(log.Data.(OUTPUT)), nil
			default:
				return nil, fmt.Errorf("unexpected error while forging log: %w", err)
			}
		}

		return output, nil
	}
}
