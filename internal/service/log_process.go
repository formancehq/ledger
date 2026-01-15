package service

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

type logProcessor[INPUT proto.Message] struct {
	operation    string
	runtimeStore store.Store
	logFactory   LogFactory
	keySetLocker KeySetLocker
	builder      func(ctx context.Context, store *unitOfWork, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error)
}

func newLogProcessor[INPUT proto.Message](
	operation string,
	runtimeStore store.Store,
	logFactory LogFactory,
	keySetLocker KeySetLocker,
	builder func(ctx context.Context, store *unitOfWork, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error),

) *logProcessor[INPUT] {
	return &logProcessor[INPUT]{
		operation:    operation,
		runtimeStore: runtimeStore,
		keySetLocker: keySetLocker,
		builder:      builder,
		logFactory:   logFactory,
	}
}

func (lp *logProcessor[INPUT]) forgeLog(
	ctx context.Context,
	ledger string,
	parameters Parameters[INPUT],
) (*ledgerpb.Log, bool, error) {

	if parameters.IdempotencyKey != "" {
		release, err := lp.keySetLocker.TryLockKeys(ctx, "ik/"+parameters.IdempotencyKey)
		if err != nil {
			return nil, false, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		id, err := lp.runtimeStore.GetLogIDForIdempotencyKey(ctx, ledger, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, false, err
		}

		if err == nil {
			log, err := lp.runtimeStore.GetLogByID(ctx, ledger, id)
			if err != nil {
				return nil, false, err
			}

			if string(ledgerpb.ComputeIdempotencyHash(parameters.Input)) != string(log.Idempotency.Hash) {
				return nil, false, ErrIdempotencyKeyConflict
			}

			return log, true, nil
		}
	}

	store := &unitOfWork{
		KeySetLocker: lp.keySetLocker,
		Store:        lp.runtimeStore,
		ledger:       ledger,
	}
	defer store.ReleaseLocks()

	input, err := lp.builder(ctx, store, parameters)
	if err != nil {
		return nil, false, err
	}

	var idp *ledgerpb.Idempotency
	if parameters.IdempotencyKey != "" {
		idp = &ledgerpb.Idempotency{
			Key:  parameters.IdempotencyKey,
			Hash: ledgerpb.ComputeIdempotencyHash(parameters.Input),
		}
	}

	log, err := lp.logFactory.CreateLog(ctx, ledger, idp, input)
	if err != nil {
		logging.FromContext(ctx).WithField("operation", lp.operation).Errorf("failed to write log: %v", err)
		return nil, false, err
	}

	return log, false, nil
}
