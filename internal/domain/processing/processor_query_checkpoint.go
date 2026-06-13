package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateQueryCheckpoint(order *raftcmdpb.CreateQueryCheckpointOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	checkpointID := s.IncrementNextQueryCheckpointID()

	cp := &raftcmdpb.QueryCheckpointState{
		CheckpointId: checkpointID,
		MaxSequence:  s.GetNextSequenceID() - 1,
		CreatedAt:    s.GetDate(),
	}

	s.SaveQueryCheckpoint(cp)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
			CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{
				CheckpointId: checkpointID,
				MaxSequence:  cp.GetMaxSequence(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteQueryCheckpoint(order *raftcmdpb.DeleteQueryCheckpointOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	if order.GetCheckpointId() == 0 {
		return nil, domain.ErrCheckpointIDRequired
	}

	s.DeleteQueryCheckpoint(order.GetCheckpointId())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedQueryCheckpoint{
			DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{
				CheckpointId: order.GetCheckpointId(),
			},
		},
	}, nil
}
