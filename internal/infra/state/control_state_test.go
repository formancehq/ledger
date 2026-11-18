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
		return &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: policy, ConfigurationOnly: true}}}
	}
	require.ErrorContains(t, apply(transition(false)), "must activate")
	require.NoError(t, apply(transition(true)))
	activated, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.True(t, activated.GetCapabilityActivations()[0].GetActive())
	machine = recoverMachineOnStore(t, store)
	require.True(t, activated.EqualVT(machine.State.ClusterPolicy))
	require.NoError(t, apply(control(configuration)))
	invalid := configuration.CloneVT()
	invalid.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: 2, Active: true}}
	require.ErrorContains(t, apply(control(invalid)), "cluster policy cannot contain capabilities")
	require.NoError(t, apply(transition(true)))
	require.NoError(t, apply(control(configuration.CloneVT())), "same config revision stays idempotent after activation")
	conflict := configuration.CloneVT()
	conflict.QueryCheckpointLimit++
	var revisionConflict *domain.ErrClusterPolicyRevisionConflict
	require.ErrorAs(t, apply(control(conflict)), &revisionConflict)
	require.ErrorContains(t, apply(transition(false)), "must activate", "deactivation is rejected")
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

func TestControlStateLegacyPolicyReplay(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachine(t)
	legacy := machine.State.ClusterPolicy.CloneVT()
	legacy.Revision++
	legacy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: 1, Active: true}}
	command := &raftcmdpb.Proposal{Id: 1, Payload: &raftcmdpb.Proposal_SetClusterPolicy{
		SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: legacy},
	}}
	encoded, err := command.MarshalVT()
	require.NoError(t, err)
	decoded := &raftcmdpb.Proposal{}
	require.NoError(t, decoded.UnmarshalVT(encoded))
	require.False(t, decoded.GetSetClusterPolicy().GetConfigurationOnly())
	result, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, decoded))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.False(t, result.Results[0].AuditEntryWritten)
	stored, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.True(t, legacy.EqualVT(stored), "released controls preserve their embedded capability semantics")
	machine = recoverMachineOnStore(t, store)
	require.True(t, legacy.EqualVT(machine.State.ClusterPolicy))
}
