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
	logReader    LogReader
	logFactory   LogFactory
	keySetLocker KeySetLocker
	builder      func(ctx context.Context, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error)
}

func newLogProcessor[INPUT any](
	operation string,
	runtimeStore RuntimeStore,
	logStore LogReader,
	logFactory LogFactory,
	keySetLocker KeySetLocker,
	builder func(ctx context.Context, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error),

) *logProcessor[INPUT] {
	return &logProcessor[INPUT]{
		operation:    operation,
		runtimeStore: runtimeStore,
		logReader:    logStore,
		keySetLocker: keySetLocker,
		builder:      builder,
		logFactory:   logFactory,
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
			log, err := lp.logReader.GetLogByID(ctx, id)
			if err != nil {
				return nil, false, err
			}

			if string(ledgerpb.ComputeIdempotencyHash(parameters.Input)) != string(hash) {
				return nil, false, ErrIdempotencyKeyConflict
			}

			return log, true, nil
		}
	}

	input, err := lp.builder(ctx, parameters)
	if err != nil {
		return nil, false, err
	}

	log, err := lp.logFactory.CreateLog(ctx, &ledgerpb.Idempotency{
		Key:  parameters.IdempotencyKey,
		Hash: ledgerpb.ComputeIdempotencyHash(parameters.Input),
	}, input)
	if err != nil {
		logging.FromContext(ctx).WithField("operation", lp.operation).Errorf("failed to write log: %v", err)
		return nil, false, err
	}

	return log, false, nil
}
