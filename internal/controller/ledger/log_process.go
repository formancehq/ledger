package ledger

import (
	"context"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/platform/postgres"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/tracing"
	"github.com/pkg/errors"
)

func runTx[INPUT any](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*ledger.Log, error)) (*ledger.Log, error) {
	var log *ledger.Log
	err := store.WithTX(ctx, nil, func(tx TX) (commit bool, err error) {
		log, err = fn(ctx, tx, parameters.Input)
		if err != nil {
			return false, err
		}
		log.IdempotencyKey = parameters.IdempotencyKey
		log.IdempotencyHash = ledger.ComputeIdempotencyHash(parameters.Input)

		_, err = tracing.TraceWithLatency(ctx, "InsertLog", func(ctx context.Context) (*struct{}, error) {
			return nil, tx.InsertLog(ctx, log)
		})
		if err != nil {
			return false, errors.Wrap(err, "failed to insert log")
		}
		logging.FromContext(ctx).Debugf("log inserted with id %d", log.ID)

		if parameters.DryRun {
			return false, nil
		}

		return true, nil
	})
	return log, err
}

// todo: handle too many clients error
func forgeLog[INPUT any](ctx context.Context, store Store, parameters Parameters[INPUT], fn func(ctx context.Context, sqlTX TX, input INPUT) (*ledger.Log, error)) (*ledger.Log, error) {
	if parameters.IdempotencyKey != "" {
		log, err := store.ReadLogWithIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, postgres.ErrNotFound) {
			return nil, err
		}
		if err == nil {
			if computedHash := ledger.ComputeIdempotencyHash(parameters.Input); log.IdempotencyHash != computedHash {
				return nil, newErrInvalidIdempotencyInputs(log.IdempotencyKey, log.IdempotencyHash, computedHash)
			}

			return log, nil
		}
	}

	for {
		log, err := runTx(ctx, store, parameters, fn)
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

				return log, nil
			default:
				return nil, errors.Wrap(err, "unexpected error while forging log")
			}
		}

		return log, nil
	}
}
