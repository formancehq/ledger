package service

import (
	"context"
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"google.golang.org/protobuf/proto"
)

// ForgeActionBuilder is a function that builds an AnyCommand from input.
type ForgeActionBuilder func(ctx context.Context, uow *unitOfWork, input proto.Message) (*raftcmdpb.AnyCommand, error)

// checkIdempotency checks if a log already exists for the given idempotency key.
// Returns the cached log if found and valid, nil if not found, or an error if conflict.
func checkIdempotency(
	ctx context.Context,
	uow *unitOfWork,
	idempotencyKey string,
	input proto.Message,
) (*commonpb.Log, error) {
	if idempotencyKey == "" {
		return nil, nil
	}

	// System-level idempotency lock
	_, err := uow.LockKeys(ctx, "ik/"+idempotencyKey)
	if err != nil {
		return nil, errors.Join(ErrIdempotencyKeyConflict, err)
	}

	sequence, err := uow.GetSequenceForIdempotencyKey(idempotencyKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	if sequence == 0 {
		return nil, nil
	}

	log, err := uow.Store.GetLogBySequence(sequence)
	if err != nil {
		return nil, err
	}

	if log.Idempotency == nil || string(commonpb.ComputeIdempotencyHash(input)) != string(log.Idempotency.Hash) {
		return nil, ErrIdempotencyKeyConflict
	}

	// Return cached log (idempotent response)
	return log, nil
}
