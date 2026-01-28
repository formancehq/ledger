package service

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
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

func (lp *logProcessor[INPUT]) forgeLog(
	ctx context.Context,
	ledgerID uint32,
	parameters Parameters[INPUT],
) (*ledgerpb.Log, bool, error) {

	if parameters.IdempotencyKey != "" {
		release, err := lp.keySetLocker.TryLockKeys(ctx, ledgerID, "ik/"+parameters.IdempotencyKey)
		if err != nil {
			return nil, false, errors.Join(ErrIdempotencyKeyConflict, err)
		}
		defer release()

		id, err := lp.store.GetLogIDForIdempotencyKey(ctx, ledgerID, parameters.IdempotencyKey)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, false, err
		}

		if err == nil {
			log, err := lp.store.GetLogByID(ctx, ledgerID, id)
			if err != nil {
				return nil, false, err
			}

			if string(ledgerpb.ComputeIdempotencyHash(parameters.Input)) != string(log.Idempotency.Hash) {
				return nil, false, ErrIdempotencyKeyConflict
			}

			return log, true, nil
		}
	}

	uow := &unitOfWork{
		KeySetLocker: lp.keySetLocker,
		Store:        lp.store,
	}
	defer uow.ReleaseLocks()

	input, err := lp.builder(ctx, uow, ledgerID, parameters)
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

	// Build the command
	cmd := newCreateLogCommand(input, ledgerID, idp)

	ret, err := lp.engine.Apply(ctx, cmd)
	if err != nil {
		lp.logger.WithField("operation", lp.operation).Errorf("failed to write log: %v", err)
		return nil, false, err
	}

	// Extract the log from the result (first element of the results array)
	results := ret.([]any)
	log := results[0].(*ledgerpb.Log)

	return log, false, nil
}

// newCreateLogCommand creates a new CreateLog command
func newCreateLogCommand(input *ledgerpb.CommandInput, ledgerID uint32, idempotency *ledgerpb.Idempotency) *ledgerpb.Command {
	createLogCmd := &ledgerpb.CreateLogCommand{
		Input:       input,
		Idempotency: idempotency,
		LedgerId:    ledgerID,
	}

	data, err := proto.Marshal(createLogCmd)
	if err != nil {
		panic(err)
	}

	action := &ledgerpb.Action{
		ActionType: ledgerpb.ActionType_CreateLog,
		Data:       data,
	}

	return &ledgerpb.Command{
		Id:      generateRandomID(),
		Actions: []*ledgerpb.Action{action},
		Date:    ledgerpb.NewTimestamp(time.Now()),
	}
}

// generateRandomID generates a random uint64 ID
func generateRandomID() uint64 {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}
