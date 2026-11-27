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

func TestMembershipRequiredIDsIncludesOnlyActive(t *testing.T) {
	t.Parallel()
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{
		{Id: 9, Active: true}, {Id: 4, Active: true}, {Id: 7},
	}}
	require.Equal(t, []uint32{4, 9}, MembershipRequiredIDs(policy))
	require.Equal(t, []uint32{4, 9}, ActiveIDs(policy))
}

func TestValidateSupportedActiveRefusesDowngrade(t *testing.T) {
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{
		{Id: 7, Active: true},
		{Id: 8, Active: false},
	}}
	require.NoError(t, ValidateSupportedActive(policy, []CapabilityID{7}))
	require.ErrorContains(t, ValidateSupportedActive(policy, nil), "active capability 7")
}

func TestValidateSupportedActiveAllowsUnsupportedInactiveCapability(t *testing.T) {
	t.Parallel()
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7}}}
	require.NoError(t, ValidateSupportedActive(policy, nil))
	require.NoError(t, ValidateSupportedActive(policy, []CapabilityID{7}))
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

func TestEvaluateRejectsInactiveCapabilityBeforeInspectingMembers(t *testing.T) {
	decision := Evaluate(capabilityState(), []CapabilityID{7}, true, []Member{{
		NodeID:       1,
		Role:         Voter,
		Reachable:    true,
		Capabilities: []CapabilityID{7},
	}})

	require.False(t, decision.Allowed)
	require.Contains(t, decision.Reason, "unsupported")
	require.Empty(t, decision.Blockers[0].NodeID, "policy blockers do not identify a member")
}

func TestEvaluateRejectsUnreachableMemberEvenWhenCapabilityIsAdvertised(t *testing.T) {
	state := capabilityState()
	state.Activations[7] = true
	decision := Evaluate(state, []CapabilityID{7}, true, []Member{{
		NodeID:       1,
		Role:         Voter,
		Reachable:    false,
		Capabilities: []CapabilityID{7},
	}})

	require.False(t, decision.Allowed)
	require.Len(t, decision.Blockers, 1)
	require.Equal(t, uint64(1), decision.Blockers[0].NodeID)
}

func TestEvaluateAllowsActiveCapabilityWhenAllMembersAdvertiseAndReachIt(t *testing.T) {
	state := capabilityState()
	state.Activations[7] = true
	decision := Evaluate(state, []CapabilityID{7}, true, []Member{
		{NodeID: 1, Role: Voter, Reachable: true, Capabilities: []CapabilityID{7}},
		{NodeID: 2, Role: Learner, Reachable: true, Capabilities: []CapabilityID{7}},
	})

	require.True(t, decision.Allowed)
	require.Empty(t, decision.Blockers)
}

func TestPrepareProposalClearsFutureDeclarations(t *testing.T) {
	proposal := &raftcmdpb.Proposal{RequiredCapabilities: []uint32{99}}
	prepared, err := PrepareProposal(proposal)
	require.NoError(t, err)
	require.Empty(t, prepared.RequiredCapabilities())
	require.Empty(t, proposal.GetRequiredCapabilities())
}

func TestPrepareProposalMarksTransactionSemantics(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
		}},
	}}}
	proposal := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{order}}}}
	prepared, err := PrepareProposal(proposal)
	require.NoError(t, err)
	require.Equal(t, []CapabilityID{StrictAccountTypes}, prepared.RequiredCapabilities())
	require.Equal(t, []uint32{uint32(StrictAccountTypes)}, proposal.GetRequiredCapabilities())
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, ValidateFinalProposal(data, prepared.RequiredCapabilities()))

	proposal.RequiredCapabilities = nil
	data, err = proposal.MarshalVT()
	require.NoError(t, err)
	require.ErrorIs(t, ValidateFinalProposal(data, prepared.RequiredCapabilities()), ErrMissingCapability)

	order.GetLedgerScoped().GetApply().Data = &raftcmdpb.LedgerApplyOrder_ExecuteNumscript{ExecuteNumscript: &raftcmdpb.ExecuteNumscriptOrder{}}
	prepared, err = PrepareProposal(proposal)
	require.NoError(t, err)
	require.Equal(t, []CapabilityID{StrictAccountTypes}, prepared.RequiredCapabilities())
}

func TestPrepareProposalMarksOnlyTransactionInMixedBatch(t *testing.T) {
	t.Parallel()
	create := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
	}}}
	transaction := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{}},
		}},
	}}}
	proposal := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{Orders: []*raftcmdpb.Order{create, transaction}}}}
	prepared, err := PrepareProposal(proposal)
	require.NoError(t, err)
	require.Equal(t, []CapabilityID{StrictAccountTypes}, prepared.RequiredCapabilities())
	require.Equal(t, []uint32{1}, proposal.GetRequiredCapabilities())
	proposal.GetBatch().Orders = []*raftcmdpb.Order{create}
	prepared, err = PrepareProposal(proposal)
	require.NoError(t, err)
	require.Empty(t, prepared.RequiredCapabilities())
	require.Empty(t, proposal.GetRequiredCapabilities())
}

func TestStrictAccountTypesActivation(t *testing.T) {
	t.Parallel()
	member := Member{NodeID: 1, Role: Voter, Reachable: true, Capabilities: SupportedIDs()}
	state := capabilityState()
	require.False(t, Evaluate(state, []CapabilityID{StrictAccountTypes}, true, []Member{member}).Allowed)
	state.Activations[StrictAccountTypes] = true
	require.True(t, Evaluate(state, []CapabilityID{StrictAccountTypes}, true, []Member{member}).Allowed)
	member.Capabilities = nil
	require.False(t, Evaluate(state, []CapabilityID{StrictAccountTypes}, true, []Member{member}).Allowed)
}

func TestFromPolicyRoundTrip(t *testing.T) {
	state := FromPolicy(&commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 99, Active: true}},
	})
	require.True(t, state.Activations[99])
	require.NoError(t, state.Validate())
}

func TestActiveIDsReturnsSortedActiveIDs(t *testing.T) {
	t.Parallel()

	policy := &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{
			{Id: 9, Active: true},
			nil,
			{Id: 3, Active: false},
			{Id: 4, Active: true},
		},
	}

	require.Equal(t, []uint32{4, 9}, ActiveIDs(policy))
	require.Nil(t, ActiveIDs(nil))
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
