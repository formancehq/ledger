package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func clusterPolicyOrder(policy *commonpb.ClusterPolicy) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
					SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy},
				},
			},
		},
	}
}

// A higher revision applies: the policy is staged and the audit log carries it.
func TestProcessSetClusterPolicy_HigherRevisionApplies(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	newPolicy := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, IdempotencyTtlMicros: 1000, QueryCheckpointLimit: 5})
	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 1})
	mockStore.EXPECT().SetClusterPolicy(newPolicy)

	result, procErr := processor.ProcessOrder(clusterPolicyOrder(newPolicy), mockStore)
	require.Nil(t, procErr)
	require.NotNil(t, result)

	log := result.GetSetClusterPolicy()
	require.NotNil(t, log)
	require.True(t, log.GetPolicy().EqualVT(newPolicy))
}

// Same revision + identical payload is an idempotent no-op: no log, no write.
func TestProcessSetClusterPolicy_SameRevisionSamePayloadNoOp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	applied := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 4, IdempotencyTtlMicros: 2000, QueryCheckpointLimit: 7})
	mockStore.EXPECT().GetClusterPolicy().Return(applied)

	result, procErr := processor.ProcessOrder(clusterPolicyOrder(applied.CloneVT()), mockStore)
	require.Nil(t, procErr)
	require.Nil(t, result)
}

// Same revision + different payload is a contract violation, rejected loudly.
func TestProcessSetClusterPolicy_SameRevisionDifferentPayloadConflict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{Revision: 4, QueryCheckpointLimit: 1})

	order := clusterPolicyOrder(withMetadataLimits(&commonpb.ClusterPolicy{Revision: 4, QueryCheckpointLimit: 2}))
	_, procErr := processor.ProcessOrder(order, mockStore)

	var conflict *domain.ErrClusterPolicyRevisionConflict
	require.ErrorAs(t, procErr, &conflict)
	require.Equal(t, uint64(4), conflict.Revision)
}

// A lower revision is stale: a newer policy already won.
func TestProcessSetClusterPolicy_LowerRevisionStale(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetClusterPolicy().Return(&commonpb.ClusterPolicy{Revision: 5, QueryCheckpointLimit: 1})

	order := clusterPolicyOrder(withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1}))
	_, procErr := processor.ProcessOrder(order, mockStore)

	var stale *domain.ErrStaleClusterPolicy
	require.ErrorAs(t, procErr, &stale)
	require.Equal(t, uint64(3), stale.ProposedRevision)
	require.Equal(t, uint64(5), stale.AppliedRevision)
}

// A higher policy revision cannot lower the replay floor. The floor protects
// restart and restore paths, so accepting a rollback here would let a later
// policy make an old binary appear safe again.
func TestProcessSetClusterPolicy_ReplayFloorCannotDecrease(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	current := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 2, MinimumReplayProtocol: capabilities.CurrentReplayProtocol, QueryCheckpointLimit: 1})
	mockStore.EXPECT().GetClusterPolicy().Return(current)

	proposed := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, MinimumReplayProtocol: capabilities.CurrentReplayProtocol - 1, QueryCheckpointLimit: 1})
	_, procErr := processor.ProcessOrder(clusterPolicyOrder(proposed), mockStore)

	var invalid *domain.ErrClusterPolicyInvalid
	require.ErrorAs(t, procErr, &invalid)
	require.Contains(t, invalid.Detail, "replay protocol floor cannot decrease")
}

// A policy cannot commit a replay floor newer than the protocol implemented
// by this binary: otherwise the node would accept the entry and then be unable
// to replay its own store after restart.
func TestProcessSetClusterPolicy_ReplayFloorCannotExceedLocalProtocol(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	current := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 2, QueryCheckpointLimit: 1})
	mockStore.EXPECT().GetClusterPolicy().Return(current)

	proposed := withMetadataLimits(&commonpb.ClusterPolicy{
		Revision:              3,
		MinimumReplayProtocol: capabilities.CurrentReplayProtocol + 1,
		QueryCheckpointLimit:  1,
	})
	_, procErr := processor.ProcessOrder(clusterPolicyOrder(proposed), mockStore)

	var invalid *domain.ErrClusterPolicyInvalid
	require.ErrorAs(t, procErr, &invalid)
	require.Contains(t, invalid.Detail, "persisted floor")
}

func TestProcessSetClusterPolicy_CapabilityActivationIsMonotonic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	current := withMetadataLimits(&commonpb.ClusterPolicy{
		Revision:                  2,
		CapabilityActivationArmed: true,
		CapabilityActivations:     []*commonpb.CapabilityActivation{{Id: 7, Active: true}},
		QueryCheckpointLimit:      1,
	})
	mockStore.EXPECT().GetClusterPolicy().Return(current)

	proposed := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1})
	_, procErr := processor.ProcessOrder(clusterPolicyOrder(proposed), mockStore)

	var invalid *domain.ErrClusterPolicyInvalid
	require.ErrorAs(t, procErr, &invalid)
	require.Contains(t, invalid.Detail, "cannot be disarmed")
}

func TestProcessSetClusterPolicy_CapabilityCannotDisappear(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	current := withMetadataLimits(&commonpb.ClusterPolicy{
		Revision:              2,
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7, Active: true}},
		QueryCheckpointLimit:  1,
	})
	mockStore.EXPECT().GetClusterPolicy().Return(current)

	proposed := withMetadataLimits(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1})
	_, procErr := processor.ProcessOrder(clusterPolicyOrder(proposed), mockStore)

	var invalid *domain.ErrClusterPolicyInvalid
	require.ErrorAs(t, procErr, &invalid)
	require.Contains(t, invalid.Detail, "capability 7 cannot be deactivated")
}

// Structural validation runs before the revision check, so it needs no applied
// policy: a missing policy, a zero revision, and a zero limit each reject.
func TestProcessSetClusterPolicy_StructuralValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		policy *commonpb.ClusterPolicy
	}{
		{"missing policy", nil},
		{"zero revision", &commonpb.ClusterPolicy{Revision: 0, QueryCheckpointLimit: 1}},
		{"zero limit", &commonpb.ClusterPolicy{Revision: 1, QueryCheckpointLimit: 0}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			mockStore := NewMockScope(ctrl)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)

			_, procErr := processor.ProcessOrder(clusterPolicyOrder(tc.policy), mockStore)

			var invalid *domain.ErrClusterPolicyInvalid
			require.ErrorAs(t, procErr, &invalid)
		})
	}
}
