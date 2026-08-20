package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processCreateQueryCheckpoint applies a CreateQueryCheckpoint order
// unconditionally: the live-checkpoint limit is enforced softly at admission, so
// a committed create always applies identically on every node regardless of that
// node's configured limit (FSM determinism).
func processCreateQueryCheckpoint(order *raftcmdpb.CreateQueryCheckpointOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	checkpointID := s.IncrementNextQueryCheckpointID()

	cp := &raftcmdpb.QueryCheckpointState{
		CheckpointId: checkpointID,
		MaxSequence:  s.GetNextSequenceID() - 1,
		CreatedAt:    s.GetDate().Mutate(),
	}

	s.SaveQueryCheckpoint(cp)
	// QueryCheckpointSaved (post-commit checkpoint scheduler gating) is
	// derived from CreatedQueryCheckpointLog by deriveSignals.

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
			CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
				CheckpointId: checkpointID,
				MaxSequence:  cp.GetMaxSequence(),
				CreatedAt:    cp.GetCreatedAt(),
			},
		},
	}, nil
}

// processDeleteQueryCheckpoint applies a delete unconditionally (id validity and
// existence are checked softly at admission). Emitting the log on every node is
// what drives the per-node physical file cleanup.
func processDeleteQueryCheckpoint(order *raftcmdpb.DeleteQueryCheckpointOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	ctx.Scope.DeleteQueryCheckpoint(order.GetCheckpointId())
	// QueryCheckpointDeleted is derived from DeletedQueryCheckpointLog by
	// deriveSignals.

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedQueryCheckpoint{
			DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{
				CheckpointId: order.GetCheckpointId(),
			},
		},
	}, nil
}
