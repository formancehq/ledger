package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func createQueryCheckpointOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		},
	}
}

func deleteQueryCheckpointOrder(id uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_DeleteQueryCheckpoint{
					DeleteQueryCheckpoint: &raftcmdpb.DeleteQueryCheckpointOrder{CheckpointId: id},
				},
			},
		},
	}
}

// Create applies unconditionally on the FSM path — the live-checkpoint limit is
// enforced softly at admission, never here.
func TestProcessCreateQueryCheckpoint_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 42}

	mockStore.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(7))
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	mockStore.EXPECT().SaveQueryCheckpoint(gomock.Any())

	result, err := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createdLog := result.GetCreatedQueryCheckpoint()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(7), createdLog.GetCheckpointId())
	require.Equal(t, uint64(99), createdLog.GetMaxSequence())
}

// Delete applies unconditionally on the FSM path — id validity and existence are
// checked softly at admission. The tombstone drives per-node file cleanup.
func TestProcessDeleteQueryCheckpoint_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().DeleteQueryCheckpoint(uint64(5))

	result, err := processor.ProcessOrder(deleteQueryCheckpointOrder(5), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deletedLog := result.GetDeletedQueryCheckpoint()
	require.NotNil(t, deletedLog)
	require.Equal(t, uint64(5), deletedLog.GetCheckpointId())
}
