package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestControlStateIndependentRevisionsAndRecovery(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	index := uint64(0)
	apply := func(proposal *raftcmdpb.Proposal) error {
		t.Helper()
		index++
		proposal.Id = index
		result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, index, proposal))
		require.NoError(t, err)
		require.False(t, result.Results[0].AuditEntryWritten)

		return result.Results[0].Error
	}
	transition := func(active bool) *raftcmdpb.Proposal {
		return &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_TransitionCapability{TransitionCapability: &commonpb.CapabilityActivation{Id: 1, Active: active}}}
	}
	configuration := machine.State.ClusterPolicy.CloneVT()
	configuration.Revision++
	configuration.QueryCheckpointLimit = 42
	control := func(policy *commonpb.ClusterPolicy) *raftcmdpb.Proposal {
		return &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetConfiguration{SetConfiguration: &raftcmdpb.SetClusterPolicyProposal{Policy: policy}}}
	}
	require.ErrorContains(t, apply(transition(true)), "must be requested")
	require.NoError(t, apply(transition(false)))
	pending, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.True(t, pending.GetCapabilityActivations()[0].GetRequested())
	require.False(t, pending.GetCapabilityActivations()[0].GetActive())
	machine = recoverMachineOnStore(t, store)
	require.True(t, pending.EqualVT(machine.State.ClusterPolicy))
	require.NoError(t, apply(control(configuration)))
	require.NoError(t, apply(transition(true)))
	require.NoError(t, apply(control(configuration.CloneVT())), "same config revision stays idempotent after activation")
	conflict := configuration.CloneVT()
	conflict.QueryCheckpointLimit++
	var revisionConflict *domain.ErrClusterPolicyRevisionConflict
	require.ErrorAs(t, apply(control(conflict)), &revisionConflict)
	require.NoError(t, apply(transition(false)), "repeated request cannot deactivate")
	configuration.Revision++
	configuration.QueryCheckpointLimit++
	require.NoError(t, apply(control(configuration)))
	stored, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.Equal(t, configuration.GetRevision(), stored.GetRevision())
	require.Equal(t, configuration.GetQueryCheckpointLimit(), stored.GetQueryCheckpointLimit())
	require.True(t, stored.GetCapabilityActivations()[0].GetActive())
	machine = recoverMachineOnStore(t, store)
	require.True(t, stored.EqualVT(machine.State.ClusterPolicy))
}
