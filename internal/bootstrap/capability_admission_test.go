package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
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

func TestProposalCapabilityAdmissionRecoversActiveWithoutIntent(t *testing.T) {
	t.Parallel()
	proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}}}}}}})
	require.NoError(t, err)
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.StrictCreateLedgerAccountTypes), Active: true}}}
	supported := []capabilities.CapabilityID{capabilities.StrictCreateLedgerAccountTypes}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true, Capabilities: supported}}
	require.NoError(t, validateProposalCapabilityAdmissionWithSupported(policy, proposal, members, supported))
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
	supported := []capabilities.CapabilityID{capabilities.StrictCreateLedgerAccountTypes}
	for _, role := range []capabilities.MemberRole{capabilities.Voter, capabilities.Learner} {
		members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Reachable: true, Capabilities: supported}, {NodeID: 2, Role: role, Reachable: true}}
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(&commonpb.ClusterPolicy{}, proposal, members, supported), capabilities.ErrCapabilityUnsupported)
		members[1].Capabilities = supported
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(&commonpb.ClusterPolicy{}, proposal, members, supported), capabilities.ErrActivationPending)
		inactive := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.StrictCreateLedgerAccountTypes)}}}
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), capabilities.ErrActivationPending)
		configuration, err := node.NewProposal(2, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{Policy: &commonpb.ClusterPolicy{Revision: 2}, ConfigurationOnly: true},
		}})
		require.NoError(t, err)
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, configuration, members, supported))
		activation, err := node.NewProposal(3, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_CapabilityActivation{
			CapabilityActivation: &commonpb.CapabilityActivation{Id: uint32(capabilities.StrictCreateLedgerAccountTypes), Active: true},
		}})
		require.NoError(t, err)
		members[1].Capabilities = nil
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, activation, members, supported), capabilities.ErrCapabilityUnsupported)
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), capabilities.ErrCapabilityUnsupported, "a member without the required capability cannot apply the proposal")
		members[1].Capabilities = supported
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, activation, members, supported))
		require.ErrorIs(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported), capabilities.ErrActivationPending, "submission does not release the barrier before commit")
		inactive.CapabilityActivations[0].Active = true
		require.NoError(t, validateProposalCapabilityAdmissionWithSupported(inactive, proposal, members, supported))
	}
}
