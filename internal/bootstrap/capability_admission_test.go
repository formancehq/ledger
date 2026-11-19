package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities/catalog"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateActiveRejectsIncompleteMembership(t *testing.T) {
	t.Parallel()

	err := capabilities.ValidateActive(
		[]capabilities.CapabilityID{7},
		[]capabilities.CapabilityID{7},
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter}},
	)

	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
}

func TestValidateActiveAcceptsNoActivation(t *testing.T) {
	t.Parallel()

	require.NoError(t, capabilities.ValidateActive(nil, nil, nil))
}

func TestProposalCapabilityAdmissionRejectsUnmarkedCreateLedgerAfterActivation(t *testing.T) {
	t.Parallel()
	proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}}}}}}})
	require.NoError(t, err)
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}}}
	supported := []capabilities.CapabilityID{1}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: supported}}
	require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(policy, proposal, members, supported), capabilities.ErrMissingCapability)
}

func TestValidateActiveAcceptsConvergedMembership(t *testing.T) {
	t.Parallel()

	require.NoError(t, capabilities.ValidateActive(
		[]capabilities.CapabilityID{7},
		[]capabilities.CapabilityID{7},
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: []capabilities.CapabilityID{7}}},
	))
}

func TestProposalCapabilityAdmissionRejectsActivationBeforeRaft(t *testing.T) {
	t.Parallel()

	proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{
		Id: 1,
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{
					Policy: &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7, Active: true}}},
				}},
			}},
		}},
	})
	require.NoError(t, err)

	err = validateProposalCapabilityAdmission(
		&commonpb.ClusterPolicy{},
		proposal,
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter}},
	)

	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
}

func TestCapabilityActivationBarrierUntilCommitAfterDrain(t *testing.T) {
	t.Parallel()
	command := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}}},
	}}}
	proposal, err := node.NewProposal(1, command)
	require.NoError(t, err)
	supported := []capabilities.CapabilityID{7}
	for _, role := range []capabilities.MemberRole{capabilities.Voter, capabilities.Learner} {
		members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: supported}, {NodeID: 2, Role: role}}
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(&commonpb.ClusterPolicy{}, proposal, members, supported))
		members[1].Capabilities = supported
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(&commonpb.ClusterPolicy{}, proposal, members, supported), capabilities.ErrActivationPending)
		inactive := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 7}}}
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), capabilities.ErrActivationPending)
		configuration, err := node.NewProposal(2, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 2}, ConfigurationOnly: true},
		}})
		require.NoError(t, err)
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, configuration, members, supported))
		activation, err := node.NewProposal(3, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_CapabilityActivation{
			CapabilityActivation: &commonpb.CapabilityActivation{Id: 7, Active: true},
		}})
		require.NoError(t, err)
		members[1].Capabilities = nil
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, activation, members, supported), capabilities.ErrCapabilityUnsupported)
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), "an uncommitted attempt does not create an irrevocable intent")
		members[1].Capabilities = supported
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, activation, members, supported))
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), capabilities.ErrActivationPending, "submission does not release the barrier before commit")
		inactive.CapabilityActivations[0].Active = true
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported))
	}
}

func TestProposalCapabilityAdmissionRejectsInactiveCapability(t *testing.T) {
	t.Parallel()

	proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{
		RequiredCapabilities: []uint32{99},
	})
	require.NoError(t, err)

	err = validateProposalCapabilityAdmission(
		&commonpb.ClusterPolicy{},
		proposal,
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true}},
	)
	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
}

func TestProposalCapabilityAdmissionSelectsCreateLedgerValidation(t *testing.T) {
	t.Parallel()
	const id = catalog.CreateLedgerAccountTypesV2
	active := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(id), Active: true}}}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true, Capabilities: []capabilities.CapabilityID{id}}}
	command := &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}}},
	}}}

	unmarked, err := node.NewProposal(1, command)
	require.NoError(t, err)
	require.ErrorIs(t, validateProposalCapabilityAdmission(&commonpb.ClusterPolicy{}, unmarked, members), capabilities.ErrActivationPending)
	require.ErrorIs(t, validateProposalCapabilityAdmission(active, unmarked, members), capabilities.ErrMissingCapability)

	catalog.SelectCreateLedgerValidation(command, active)
	marked, err := node.NewProposal(2, command)
	require.NoError(t, err)
	require.Equal(t, []capabilities.CapabilityID{id}, marked.RequiredCapabilities())
	require.NoError(t, validateProposalCapabilityAdmission(active, marked, members))
	require.ErrorIs(t, validateProposalCapabilityAdmission(&commonpb.ClusterPolicy{}, marked, members), capabilities.ErrActivationPending)
	require.ErrorIs(t, validateProposalCapabilityAdmission(active, marked, []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true}}), capabilities.ErrCapabilityUnsupported)
}
