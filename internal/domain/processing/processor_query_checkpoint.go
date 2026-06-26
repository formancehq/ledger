package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

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
			},
		},
	}, nil
}

func processDeleteQueryCheckpoint(order *raftcmdpb.DeleteQueryCheckpointOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	if order.GetCheckpointId() == 0 {
		return nil, domain.ErrCheckpointIDRequired
	}

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
