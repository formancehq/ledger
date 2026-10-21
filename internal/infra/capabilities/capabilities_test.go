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
	state := armedState(FeatureOnly)
	state.Activations[IDExecutionPlanV1] = true
	decision := Evaluate(state, []ID{IDExecutionPlanV1}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
		{NodeID: 2, Role: Learner, Reachable: true},
	})
	require.False(t, decision.Allowed)
	require.Equal(t, uint64(2), decision.Blockers[0].NodeID)
}

func TestFeatureOnlyBlocksInactiveRequiredCapability(t *testing.T) {
	decision := Evaluate(armedState(FeatureOnly), []ID{IDExecutionPlanV1}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
		{NodeID: 2, Role: Learner, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
	})
	require.False(t, decision.Allowed)
	require.Contains(t, decision.Reason, ErrCapabilityUnsupported.Error())
	require.Contains(t, decision.Blockers, Blocker{Reason: "capability 1 is inactive"})
}

func TestFeatureOnlyAllowsActiveRequiredCapabilityWhenMembersSupport(t *testing.T) {
	state := armedState(FeatureOnly)
	state.Activations[IDExecutionPlanV1] = true
	decision := Evaluate(state, []ID{IDExecutionPlanV1}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
		{NodeID: 2, Role: Learner, Reachable: true, Capabilities: []ID{IDExecutionPlanV1}},
	})
	require.True(t, decision.Allowed)
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
	data, err := (&raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{}, RequiredCapabilities: []uint32{uint32(IDExecutionPlanV1)}}).MarshalVT()
	require.NoError(t, err)
	require.ErrorIs(t, ValidateFinalProposal(data, nil), ErrMissingCapability)
}

func TestValidateFinalProposalRejectsSemanticDependencyOmittedFromWire(t *testing.T) {
	data, err := (&raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{}}).MarshalVT()
	require.NoError(t, err)

	require.ErrorIs(t, ValidateFinalProposal(data, nil), ErrMissingCapability)
}

func TestValidateFinalProposalRequiresAllSemanticDependencies(t *testing.T) {
	proposal := &raftcmdpb.Proposal{
		ExecutionPlan: &raftcmdpb.ExecutionPlan{},
		Idempotency:   &commonpb.Idempotency{Key: "retry"},
	}
	require.NoError(t, StampProposal(proposal))
	require.Equal(t, []uint32{uint32(IDExecutionPlanV1), uint32(IDSemanticReplayV1)}, proposal.GetRequiredCapabilities())

	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, ValidateFinalProposal(data, []ID{IDSemanticReplayV1, IDExecutionPlanV1}))

	proposal.RequiredCapabilities = []uint32{uint32(IDExecutionPlanV1)}
	data, err = proposal.MarshalVT()
	require.NoError(t, err)
	require.ErrorIs(t, ValidateFinalProposal(data, []ID{IDExecutionPlanV1}), ErrMissingCapability)
}

func TestRequiredCapabilitiesIsDeterministicForEmptyExecutionPlan(t *testing.T) {
	required, err := RequiredCapabilities(&raftcmdpb.Proposal{ExecutionPlan: &raftcmdpb.ExecutionPlan{}})
	require.NoError(t, err)
	require.Equal(t, []ID{IDExecutionPlanV1}, required)
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
