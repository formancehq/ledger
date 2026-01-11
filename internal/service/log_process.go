package service

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

type logProcessor[INPUT any] struct {
	operation    string
	runtimeStore RuntimeStore
	logStore LogStore
	keySetLocker KeySetLocker
	builder      func(ctx context.Context, parameters Parameters[INPUT]) (*ledgerpb.Log, error)
}

func newLogProcessor[INPUT any](
	operation string,
	runtimeStore RuntimeStore,
	logStore LogStore,
	keySetLocker KeySetLocker,
	builder func(ctx context.Context, parameters Parameters[INPUT]) (*ledgerpb.Log, error),

) *logProcessor[INPUT] {
	return &logProcessor[INPUT]{
		operation:    operation,
		runtimeStore: runtimeStore,
		logStore:     logStore,
		keySetLocker: keySetLocker,
		builder:      builder,
	}
}

func (lp *logProcessor[INPUT]) forgeLog(
	ctx context.Context,
	parameters Parameters[INPUT],
) (*ledgerpb.Log, bool, error) {

	if parameters.IdempotencyKey != "" {

		release, err := lp.keySetLocker.TryLockKeys(ctx, "ik/"+parameters.IdempotencyKey)
		if err != nil {
			return nil, false, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		hash, id, err := lp.runtimeStore.GetLogForIdempotencyKey(ctx, parameters.IdempotencyKey)
		if err != nil {
			return nil, false, err
		}

		if id != 0 {
			log, err := lp.logStore.GetLogByID(ctx, id)
			if err != nil {
				return nil, false, err
			}

			if ledgerpb.ComputeIdempotencyHash(parameters.Input) != hash {
				return nil, false, ErrIdempotencyKeyConflict
			}

			return log, true, nil
		}
	}

	log, err := lp.builder(ctx, parameters)
	if err != nil {
		return nil, false, err
	}

	if parameters.DryRun {
		return log, false, nil
	}

	if err := lp.logStore.InsertLogs(ctx, log); err != nil {
		logging.FromContext(ctx).WithField("operation", lp.operation).Errorf("failed to write log: %v", err)
		return nil, false, err
	}

	return log, false, nil
}
