package capabilities

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func armedState(policy AdmissionPolicy) State {
	return State{Armed: true, Policy: policy, MembershipRevision: 7, Activations: map[ID]bool{IDExecutionPlanV1: false}}
}

func TestEvaluateIncludesVotersAndLearners(t *testing.T) {
	decision := Evaluate(armedState(FeatureOnly), []ID{IDExecutionPlanV1}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
		{NodeID: 2, Role: Learner, Reachable: true},
	})
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}

func TestAllWritesBlocksUnrelatedBusinessWrite(t *testing.T) {
	decision := Evaluate(armedState(AllWrites), nil, true, nil)
	require.False(t, decision.Allowed)
	require.Contains(t, decision.Reason, "business writes")
}

func TestFeatureOnlyAllowsUnrelatedWrite(t *testing.T) {
	decision := Evaluate(armedState(FeatureOnly), nil, true, []Member{{NodeID: 1, Role: Voter, Reachable: true}})
	require.True(t, decision.Allowed)
}

func TestValidateFinalProposalRejectsMissingDeclaration(t *testing.T) {
	data, err := (&raftcmdpb.Proposal{RequiredCapabilities: []uint32{uint32(IDExecutionPlanV1)}}).MarshalVT()
	require.NoError(t, err)
	require.ErrorIs(t, ValidateFinalProposal(data, nil), ErrMissingCapability)
}

func TestFromPolicyRoundTrip(t *testing.T) {
	state := FromPolicy(&commonpb.ClusterPolicy{
		CapabilityActivationArmed:    true,
		CapabilityAdmissionPolicy:    commonpb.CapabilityAdmissionPolicy_CAPABILITY_ADMISSION_POLICY_ALL_WRITES,
		MinimumReplayProtocol:        4,
		CapabilityMembershipRevision: 9,
		CapabilityActivations:        []*commonpb.CapabilityActivation{{Id: uint32(IDExecutionPlanV1), Active: true}},
	})
	require.Equal(t, AllWrites, state.Policy)
	require.True(t, state.Activations[IDExecutionPlanV1])
	require.NoError(t, state.Validate())
}
