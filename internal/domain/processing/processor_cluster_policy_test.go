package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
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

	newPolicy := &commonpb.ClusterPolicy{Revision: 3, IdempotencyTtlMicros: 1000, QueryCheckpointLimit: 5}
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

	applied := &commonpb.ClusterPolicy{Revision: 4, IdempotencyTtlMicros: 2000, QueryCheckpointLimit: 7}
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

	order := clusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 4, QueryCheckpointLimit: 2})
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

	order := clusterPolicyOrder(&commonpb.ClusterPolicy{Revision: 3, QueryCheckpointLimit: 1})
	_, procErr := processor.ProcessOrder(order, mockStore)

	var stale *domain.ErrStaleClusterPolicy
	require.ErrorAs(t, procErr, &stale)
	require.Equal(t, uint64(3), stale.ProposedRevision)
	require.Equal(t, uint64(5), stale.AppliedRevision)
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
