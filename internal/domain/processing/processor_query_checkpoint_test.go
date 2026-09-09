package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
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

// Below the policy cap a create is admitted and stamps id/max_sequence/created_at.
func TestProcessCreateQueryCheckpoint_UnderLimit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 42}

	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{QueryCheckpointLimit: 2})
	mockStore.EXPECT().LiveQueryCheckpointCount().Return(uint64(1))
	mockStore.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(7))
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	mockStore.EXPECT().GetRaftIndex().Return(uint64(123))
	mockStore.EXPECT().SaveQueryCheckpoint(gomock.Any())

	result, procErr := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)
	require.Nil(t, procErr)

	createdLog := result.GetCreatedQueryCheckpoint()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(7), createdLog.GetCheckpointId())
	require.Equal(t, uint64(99), createdLog.GetMaxSequence())
	require.Equal(t, uint64(42), createdLog.GetCreatedAt().GetData())
	require.Equal(t, uint64(123), createdLog.GetAppliedIndex())
}

// At the cap a create is rejected with the typed error carrying the limit; no id
// is consumed and nothing is saved.
func TestProcessCreateQueryCheckpoint_AtLimit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{QueryCheckpointLimit: 2})
	mockStore.EXPECT().LiveQueryCheckpointCount().Return(uint64(2))

	_, procErr := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)

	var limitErr *domain.ErrCheckpointLimitReached
	require.ErrorAs(t, procErr, &limitErr)
	require.Equal(t, uint64(2), limitErr.Limit)
}

// An unset limit (0, before any policy is committed) is uncapped: the live count
// is not even consulted.
func TestProcessCreateQueryCheckpoint_UncappedWhenLimitZero(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1}

	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{QueryCheckpointLimit: 0})
	mockStore.EXPECT().IncrementNextQueryCheckpointID().Return(uint64(1))
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	mockStore.EXPECT().GetRaftIndex().Return(uint64(123))
	mockStore.EXPECT().SaveQueryCheckpoint(gomock.Any())

	result, procErr := processor.ProcessOrder(createQueryCheckpointOrder(), mockStore)
	require.Nil(t, procErr)
	require.NotNil(t, result.GetCreatedQueryCheckpoint())
}

// A delete of a live checkpoint is admitted and emits the tombstone.
func TestProcessDeleteQueryCheckpoint_Live(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().QueryCheckpointExists(uint64(5)).Return(true)
	mockStore.EXPECT().DeleteQueryCheckpoint(uint64(5))

	result, procErr := processor.ProcessOrder(deleteQueryCheckpointOrder(5), mockStore)
	require.Nil(t, procErr)
	require.Equal(t, uint64(5), result.GetDeletedQueryCheckpoint().GetCheckpointId())
}

// A delete of a non-live id is rejected as not-found; nothing is deleted.
func TestProcessDeleteQueryCheckpoint_NotLive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().QueryCheckpointExists(uint64(5)).Return(false)

	_, procErr := processor.ProcessOrder(deleteQueryCheckpointOrder(5), mockStore)

	var notFound *domain.ErrCheckpointNotFound
	require.ErrorAs(t, procErr, &notFound)
	require.Equal(t, uint64(5), notFound.CheckpointID)
}

// A zero id is rejected structurally before any state is consulted.
func TestProcessDeleteQueryCheckpoint_ZeroID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	_, procErr := processor.ProcessOrder(deleteQueryCheckpointOrder(0), mockStore)
	require.ErrorIs(t, procErr, domain.ErrCheckpointIDRequired)
}
