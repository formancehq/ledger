package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processCreateQueryCheckpoint applies a create after the idempotency gate,
// enforcing the replicated query-checkpoint cap inside the FSM. The cap is read
// from the committed ClusterPolicy and the live count from deterministic FSM
// state, so a committed create resolves identically on every node; a keyed retry
// replays the frozen outcome before reaching here, so an accepted create that
// filled the cap still returns its original success on retry.
func processCreateQueryCheckpoint(order *raftcmdpb.CreateQueryCheckpointOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	// A committed policy always carries a limit >= 1; limit 0 is the unset
	// default before any policy is committed and means "no cap" (write readiness
	// gates business writes until a policy is committed, so a real create sees a
	// configured limit).
	limit := s.GetClusterPolicy().GetQueryCheckpointLimit()
	if limit != 0 && s.LiveQueryCheckpointCount() >= limit {
		return nil, &domain.ErrCheckpointLimitReached{Limit: limit}
	}

	checkpointID := s.IncrementNextQueryCheckpointID()

	cp := &raftcmdpb.QueryCheckpointState{
		CheckpointId: checkpointID,
		MaxSequence:  s.GetNextSequenceID() - 1,
		CreatedAt:    s.GetDate().Mutate(),
		AppliedIndex: s.GetRaftIndex(),
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
				AppliedIndex: cp.GetAppliedIndex(),
			},
		},
	}, nil
}

// processDeleteQueryCheckpoint applies a delete after the idempotency gate,
// rejecting a non-live id inside the FSM so the outcome is deterministic and a
// keyed retry of a successful delete replays that success.
func processDeleteQueryCheckpoint(order *raftcmdpb.DeleteQueryCheckpointOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	id := order.GetCheckpointId()
	if id == 0 {
		return nil, domain.ErrCheckpointIDRequired
	}

	if !ctx.Scope.QueryCheckpointExists(id) {
		return nil, &domain.ErrCheckpointNotFound{CheckpointID: id}
	}

	ctx.Scope.DeleteQueryCheckpoint(id)
	// QueryCheckpointDeleted is derived from DeletedQueryCheckpointLog by
	// deriveSignals.

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedQueryCheckpoint{
			DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{
				CheckpointId: id,
			},
		},
	}, nil
}
