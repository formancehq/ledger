package service

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

type logProcessor struct {
	operation    string
	store        store.Store
	engine       Engine
	keySetLocker KeySetLocker
	logger       logging.Logger
	builder      func(ctx context.Context, store *unitOfWork, ledgerID uint32, input proto.Message) (*raftcmdpb.CommandInput, error)
}

func newLogProcessor(
	operation string,
	store store.Store,
	engine Engine,
	keySetLocker KeySetLocker,
	logger logging.Logger,
	builder func(ctx context.Context, store *unitOfWork, ledgerID uint32, input proto.Message) (*raftcmdpb.CommandInput, error),
) *logProcessor {
	return &logProcessor{
		operation:    operation,
		store:        store,
		keySetLocker: keySetLocker,
		builder:      builder,
		engine:       engine,
		logger:       logger,
	}
}

func (lp *logProcessor) forgeAction(
	ctx context.Context,
	ledgerID uint32,
	idempotencyKey string,
	input proto.Message,
) (*raftcmdpb.Action, *commonpb.Log, error) {

	if idempotencyKey != "" {
		release, err := lp.keySetLocker.TryLockKeys(ctx, ledgerID, "ik/"+idempotencyKey)
		if err != nil {
			return nil, nil, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		id, err := lp.store.GetLogIDForIdempotencyKey(ctx, ledgerID, idempotencyKey)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, nil, err
		}

		if err == nil {
			log, err := lp.store.GetLogByID(ctx, ledgerID, id)
			if err != nil {
				return nil, nil, err
			}

			if string(commonpb.ComputeIdempotencyHash(input)) != string(log.Idempotency.Hash) {
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

	commandInput, err := lp.builder(ctx, uow, ledgerID, input)
	if err != nil {
		return nil, nil, err
	}

	var idp *commonpb.Idempotency
	if idempotencyKey != "" {
		idp = &commonpb.Idempotency{
			Key:  idempotencyKey,
			Hash: commonpb.ComputeIdempotencyHash(input),
		}
	}

	// Build the action
	action := raft.NewCreateLogAction(commandInput, ledgerID, idp)

	return action, nil, nil
}

// forgeLogAndApply forges an action and applies it, returning the resulting log
func forgeLogAndApply(
	ctx context.Context,
	lp *logProcessor,
	ledgerID uint32,
	idempotencyKey string,
	input proto.Message,
	apply func(ctx context.Context, action *raftcmdpb.Action) (*commonpb.Log, error),
) (*commonpb.Log, error) {
	action, cachedLog, err := lp.forgeAction(ctx, ledgerID, idempotencyKey, input)
	if err != nil {
		return nil, err
	}
	if cachedLog != nil {
		return cachedLog, nil
	}

	return apply(ctx, action)
}
