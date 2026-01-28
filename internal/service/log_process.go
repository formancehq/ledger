package service

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

type logProcessor[INPUT proto.Message] struct {
	operation    string
	store        store.Store
	engine       Engine
	keySetLocker KeySetLocker
	logger       logging.Logger
	builder      func(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error)
}

func newLogProcessor[INPUT proto.Message](
	operation string,
	store store.Store,
	engine Engine,
	keySetLocker KeySetLocker,
	logger logging.Logger,
	builder func(ctx context.Context, store *unitOfWork, ledgerID uint32, parameters Parameters[INPUT]) (*ledgerpb.CommandInput, error),

) *logProcessor[INPUT] {
	return &logProcessor[INPUT]{
		operation:    operation,
		store:        store,
		keySetLocker: keySetLocker,
		builder:      builder,
		engine:       engine,
		logger:       logger,
	}
}

func (lp *logProcessor[INPUT]) forgeAction(
	ctx context.Context,
	ledgerID uint32,
	parameters Parameters[INPUT],
) (*ledgerpb.Action, *ledgerpb.Log, error) {

	if parameters.IdempotencyKey != "" {
		release, err := lp.keySetLocker.TryLockKeys(ctx, ledgerID, "ik/"+parameters.IdempotencyKey)
		if err != nil {
			return nil, nil, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		id, err := lp.store.GetLogIDForIdempotencyKey(ctx, ledgerID, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, nil, err
		}

		if err == nil {
			log, err := lp.store.GetLogByID(ctx, ledgerID, id)
			if err != nil {
				return nil, nil, err
			}

			if string(ledgerpb.ComputeIdempotencyHash(parameters.Input)) != string(log.Idempotency.Hash) {
				return nil, nil, ErrIdempotencyKeyConflict
			}

			// Return cached log (idempotent response)
			return nil, log, nil
		}
	}

	uow := &unitOfWork{
		KeySetLocker: lp.keySetLocker,
		Store:        lp.store,
	}
	defer uow.ReleaseLocks()

	input, err := lp.builder(ctx, uow, ledgerID, parameters)
	if err != nil {
		return nil, nil, err
	}

	var idp *ledgerpb.Idempotency
	if parameters.IdempotencyKey != "" {
		idp = &ledgerpb.Idempotency{
			Key:  parameters.IdempotencyKey,
			Hash: ledgerpb.ComputeIdempotencyHash(parameters.Input),
		}
	}

	// Build the action
	action := raft.NewCreateLogAction(input, ledgerID, idp)

	return action, nil, nil
}
