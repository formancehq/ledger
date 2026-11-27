package node

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestNewProposalBindsCapabilitiesToSerializedCommand(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 1}
	proposal, err := NewProposal(1, command)
	require.NoError(t, err)

	command.RequiredCapabilities = []uint32{99}
	require.NotEqual(t, command.GetRequiredCapabilities(), proposal.RequiredCapabilities())

	data := proposal.Data()
	data[0] ^= 0xff
	require.NotEqual(t, data, proposal.Data())
}

func TestBackupStartMarkerFollowsPreparationPolicy(t *testing.T) {
	t.Parallel()
	command := &raftcmdpb.Proposal{Id: 1, TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{BackupOrder: &raftcmdpb.BackupOrder{
			Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{JobId: 1}},
		}},
	}}}
	_, err := NewProposal(1, command)
	require.ErrorIs(t, err, capabilities.ErrInvalidDeclaration)

	unmarked, err := NewProposalWithPolicy(1, command, &commonpb.ClusterPolicy{})
	require.NoError(t, err)
	require.Empty(t, unmarked.RequiredCapabilities())
	decoded := new(raftcmdpb.Proposal)
	require.NoError(t, decoded.UnmarshalVT(unmarked.Data()))
	require.Empty(t, decoded.GetRequiredCapabilities())

	marked, err := NewProposalWithPolicy(1, command, &commonpb.ClusterPolicy{
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.BackupDestinationAlias), Active: true}},
	})
	require.NoError(t, err)
	require.Equal(t, []capabilities.CapabilityID{capabilities.BackupDestinationAlias}, marked.RequiredCapabilities())
	require.NoError(t, decoded.UnmarshalVT(marked.Data()))
	require.Equal(t, []uint32{uint32(capabilities.BackupDestinationAlias)}, decoded.GetRequiredCapabilities())
}

func TestProposalSetPredictedIndexPatchesSerializedPayload(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 1}
	proposal, err := NewProposal(1, command)
	require.NoError(t, err)

	proposal.SetPredictedIndex(42)

	decoded := new(raftcmdpb.Proposal)
	require.NoError(t, decoded.UnmarshalVT(proposal.Data()))
	require.Equal(t, uint64(42), decoded.GetPredictedIndex())
}

func TestNewProposalAppendsPredictedIndexWithoutMutatingPrefix(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 1}
	proposal, err := NewProposal(1, command)
	require.NoError(t, err)

	before := proposal.Data()
	proposal.SetPredictedIndex(42)
	after := proposal.Data()
	require.Greater(t, len(after), len(before))
	require.Equal(t, before, after[:len(before)])
}

func TestNewProposalReservesPredictedIndexCapacity(t *testing.T) {
	for _, size := range []int{77, 8192, 1048576} {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			command := &raftcmdpb.Proposal{Id: 1}
			command.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 2047, protowire.BytesType), make([]byte, size)))
			proposal, err := NewProposal(1, command)
			require.NoError(t, err)
			before := &proposal.data[0]
			require.GreaterOrEqual(t, cap(proposal.data)-len(proposal.data), protowire.SizeTag(predictedIndexField)+8)
			proposal.SetPredictedIndex(42)
			require.Same(t, before, &proposal.data[0])
		})
	}
}

func TestNewProposalDerivesActivePolicyCapabilities(t *testing.T) {
	command := &raftcmdpb.Proposal{
		Id: 1,
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{
					Policy: &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{
						{Id: 7, Active: true},
						{Id: 8, Active: false},
					}},
				}},
			}},
		}},
	}

	proposal, err := NewProposal(1, command)
	require.NoError(t, err)
	require.Equal(t, []capabilities.CapabilityID{7}, proposal.ActivePolicyCapabilities())

	active := proposal.ActivePolicyCapabilities()
	active[0] = 99
	require.Equal(t, []capabilities.CapabilityID{7}, proposal.ActivePolicyCapabilities(), "candidate metadata must be defensive")
}

func TestNewProposalDerivesActivePolicyCapabilitiesFromNativePolicy(t *testing.T) {
	command := &raftcmdpb.Proposal{
		Id: 1,
		Payload: &raftcmdpb.Proposal_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyProposal{
				Policy: &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{
					{Id: 7, Active: true},
					{Id: 8, Active: false},
				}},
			},
		},
	}

	proposal, err := NewProposal(1, command)
	require.NoError(t, err)
	require.Equal(t, []capabilities.CapabilityID{7}, proposal.ActivePolicyCapabilities())
}
