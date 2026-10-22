package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func armedState() State {
	return State{Armed: true, MembershipRevision: 7, Activations: map[ID]bool{}}
}

func TestEvaluateIncludesVotersAndLearners(t *testing.T) {
	state := armedState()
	state.Activations[99] = true
	decision := Evaluate(state, []ID{99}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{99}},
		{NodeID: 2, Role: Learner, Reachable: true},
	})
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}

func TestEvaluateAllowsUnrelatedWriteWhenNoFeatureIsDeclared(t *testing.T) {
	decision := Evaluate(armedState(), nil, true, []Member{{NodeID: 1, Role: Voter, Reachable: true}})
	require.True(t, decision.Allowed)
}

func TestValidateFinalProposalHasNoCurrentDependencies(t *testing.T) {
	data, err := (&raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{}}).MarshalVT()
	require.NoError(t, err)
	require.NoError(t, ValidateFinalProposal(data, nil))
}

func TestStampProposalClearsFutureDeclarations(t *testing.T) {
	proposal := &raftcmdpb.Proposal{RequiredCapabilities: []uint32{99}}
	require.NoError(t, StampProposal(proposal))
	require.Empty(t, proposal.GetRequiredCapabilities())
}

func TestFromPolicyRoundTrip(t *testing.T) {
	state := FromPolicy(&commonpb.ClusterPolicy{
		CapabilityActivationArmed:    true,
		MinimumReplayProtocol:        4,
		CapabilityMembershipRevision: 9,
		CapabilityActivations:        []*commonpb.CapabilityActivation{{Id: 99, Active: true}},
	})
	require.True(t, state.Armed)
	require.True(t, state.Activations[99])
	require.NoError(t, state.Validate())
}
