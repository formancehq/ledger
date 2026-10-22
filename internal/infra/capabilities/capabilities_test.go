package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func capabilityState() State {
	return State{Activations: map[CapabilityID]bool{}}
}

func TestEvaluateIncludesVotersAndLearners(t *testing.T) {
	state := capabilityState()
	state.Activations[99] = true
	decision := Evaluate(state, []CapabilityID{99}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []CapabilityID{99}},
		{NodeID: 2, Role: Learner, Reachable: true},
	})
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}

func TestEvaluateAllowsUnrelatedWriteWhenNoFeatureIsDeclared(t *testing.T) {
	decision := Evaluate(capabilityState(), nil, true, []Member{{NodeID: 1, Role: Voter, Reachable: true}})
	require.True(t, decision.Allowed)
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
		{NodeID: 1, Role: Voter, Capabilities: []CapabilityID{1, 2}},
		{NodeID: 2, Role: Learner, Capabilities: []CapabilityID{1}},
	}

	ready, err := Ready([]CapabilityID{2, 1}, members)
	require.NoError(t, err)
	require.Equal(t, []CapabilityID{1}, ready)
}

func TestReadyDoesNotActivateAnEmptyCluster(t *testing.T) {
	ready, err := Ready([]CapabilityID{1}, nil)
	require.NoError(t, err)
	require.Empty(t, ready)
}

func TestReadyRejectsMalformedMemberEvidence(t *testing.T) {
	_, err := Ready([]CapabilityID{1}, []Member{{
		NodeID:       1,
		Role:         Voter,
		Capabilities: []CapabilityID{1, 1},
	}})
	require.ErrorContains(t, err, "invalid capability evidence")
}

func TestReadyIgnoresUnsupportedAdvertisedIDs(t *testing.T) {
	ready, err := Ready([]CapabilityID{1}, []Member{{
		NodeID:       1,
		Role:         Voter,
		Capabilities: []CapabilityID{1, 99},
	}})
	require.NoError(t, err)
	require.Equal(t, []CapabilityID{1}, ready)
}
