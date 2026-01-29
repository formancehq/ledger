package service

import (
	"context"
	"errors"
	"fmt"

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
) (*raftcmdpb.Action, *commonpb.LedgerLog, error) {

	if idempotencyKey != "" {
		release, err := lp.keySetLocker.TryLockKeys(ctx, ledgerID, "ik/"+idempotencyKey)
		if err != nil {
			return nil, nil, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		sequence, err := lp.store.GetSequenceForIdempotencyKey(ctx, idempotencyKey)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, nil, err
		}

		if err == nil && sequence > 0 {
			log, err := lp.store.GetLogBySequence(ctx, sequence)
			if err != nil {
				return nil, nil, err
			}

			// Extract the ledger log from the log
			applyLog, ok := log.Payload.(*commonpb.Log_Apply)
			if !ok || applyLog.Apply == nil || applyLog.Apply.Log == nil {
				return nil, nil, fmt.Errorf("log %d does not contain an apply log", sequence)
			}
			ledgerLog := applyLog.Apply.Log

			if log.Idempotency == nil || string(commonpb.ComputeIdempotencyHash(input)) != string(log.Idempotency.Hash) {
				return nil, nil, ErrIdempotencyKeyConflict
			}

			// Return cached ledger log (idempotent response)
			return nil, ledgerLog, nil
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
