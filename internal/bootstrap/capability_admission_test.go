package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/incarnationmigration"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestDormantRestoredCheckpointKeepsBusinessAdmissionOpen(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditEntry{Sequence: 1}))
	require.NoError(t, batch.Commit())
	checkpoint, err := store.CreateTemporaryCheckpoint("legacy")
	require.NoError(t, err)
	restored, err := dal.OpenDirect(checkpoint, logging.Testing())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	require.NoError(t, incarnationmigration.MigrateBeforeRaft(t.Context(), restored))
	marker, err := incarnationmigration.ReadMarker(restored)
	require.NoError(t, err)
	require.False(t, marker.Complete)
	ids, err := incarnationmigration.AdvertisedIDs(restored)
	require.NoError(t, err)
	require.Equal(t, []uint32{uint32(capabilities.AccountIncarnationWritesV1)}, ids)
	member := capabilities.Member{NodeID: 1, Role: capabilities.Voter, Capabilities: []capabilities.CapabilityID{capabilities.CapabilityID(ids[0])}}
	proposal, err := node.NewProposal(2, &raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "restored", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
	}}}}})
	require.NoError(t, err)
	require.NoError(t, validateProposalCapabilityAdmission(&commonpb.ClusterPolicy{}, proposal, []capabilities.Member{member}))
	require.Empty(t, mustReadyForAutomaticActivation(t, []capabilities.Member{member}))

	member.Capabilities = capabilities.SupportedIDs()
	require.ErrorIs(t, validateProposalCapabilityAdmission(&commonpb.ClusterPolicy{}, proposal, []capabilities.Member{member}), capabilities.ErrActivationPending)
}

func mustReadyForAutomaticActivation(t *testing.T, members []capabilities.Member) []capabilities.CapabilityID {
	t.Helper()
	ready, err := readyForAutomaticActivation(capabilities.AutomaticallyActivatedIDs(), members)
	require.NoError(t, err)

	return ready
}

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
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}}}
	supported := []capabilities.CapabilityID{1}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: supported}}
	require.NoError(t, validateProposalCapabilityAdmissionWithSupported(policy, proposal, members, supported))
}

func TestHistoryActivationBlocksBusinessWritesUntilCommit(t *testing.T) {
	proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "ledger", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}}},
	}}})
	require.NoError(t, err)
	policy := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: capabilities.SupportedIDs()}}
	require.ErrorIs(t, validateProposalCapabilityAdmission(policy, proposal, members), capabilities.ErrActivationPending)
	policy.CapabilityActivations = append(policy.CapabilityActivations, &commonpb.CapabilityActivation{Id: uint32(capabilities.AccountIncarnationHistoryCompleteV1), Active: true})
	require.NoError(t, validateProposalCapabilityAdmission(policy, proposal, members))
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

func TestProposalCapabilityAdmissionRejectsLegacyAccountWriteAfterActivation(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 2, ExecutionPlan: &raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{{AttrCode: uint32(dal.SubAttrVolume)}}}}
	proposal, err := node.NewProposal(2, command)
	require.NoError(t, err)
	err = validateProposalCapabilityAdmission(
		&commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}},
		proposal,
		[]capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: []capabilities.CapabilityID{capabilities.AccountIncarnationWritesV1}}},
	)
	require.ErrorContains(t, err, "missing incarnation coverage")
}
