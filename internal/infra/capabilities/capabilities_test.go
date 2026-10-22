package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func capabilityState() State {
	return State{Activations: map[ID]bool{}}
}

func TestEvaluateIncludesVotersAndLearners(t *testing.T) {
	state := capabilityState()
	state.Activations[99] = true
	decision := Evaluate(state, []ID{99}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{99}},
		{NodeID: 2, Role: Learner, Reachable: true},
	})
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}

func TestEvaluateAllowsUnrelatedWriteWhenNoFeatureIsDeclared(t *testing.T) {
	decision := Evaluate(capabilityState(), nil, true, []Member{{NodeID: 1, Role: Voter, Reachable: true}})
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
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 99, Active: true}},
	})
	require.True(t, state.Activations[99])
	require.NoError(t, state.Validate())
}

func TestReadyReturnsIntersectionAcrossMembers(t *testing.T) {
	members := []Member{
		{NodeID: 1, Role: Voter, Capabilities: []ID{1, 2}},
		{NodeID: 2, Role: Learner, Capabilities: []ID{1}},
	}

	ready, err := Ready([]ID{2, 1}, members)
	require.NoError(t, err)
	require.Equal(t, []ID{1}, ready)
}

func TestReadyDoesNotActivateAnEmptyCluster(t *testing.T) {
	ready, err := Ready([]ID{1}, nil)
	require.NoError(t, err)
	require.Empty(t, ready)
}

func TestReadyRejectsMalformedMemberEvidence(t *testing.T) {
	_, err := Ready([]ID{1}, []Member{{
		NodeID:       1,
		Role:         Voter,
		Capabilities: []ID{1, 1},
	}})
	require.ErrorContains(t, err, "invalid capability evidence")
}

func TestReadyIgnoresUnsupportedAdvertisedIDs(t *testing.T) {
	ready, err := Ready([]ID{1}, []Member{{
		NodeID:       1,
		Role:         Voter,
		Capabilities: []ID{1, 99},
	}})
	require.NoError(t, err)
	require.Equal(t, []ID{1}, ready)
}
