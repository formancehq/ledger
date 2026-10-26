package node

import (
	"testing"

	"github.com/stretchr/testify/require"

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

func TestProposalSetPredictedIndexPatchesSerializedPayload(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 1}
	proposal, err := NewProposal(1, command)
	require.NoError(t, err)

	proposal.SetPredictedIndex(42)

	decoded := new(raftcmdpb.Proposal)
	require.NoError(t, decoded.UnmarshalVT(proposal.Data()))
	require.Equal(t, uint64(42), decoded.GetPredictedIndex())
}

func TestNewProposalReservesPredictedIndexSuffix(t *testing.T) {
	command := &raftcmdpb.Proposal{Id: 1}
	proposal, err := NewProposal(1, command)
	require.NoError(t, err)

	before := proposal.Data()
	reserved := cap(proposal.data)
	require.GreaterOrEqual(t, reserved-len(proposal.data), predictedIndexEncodedSize)

	proposal.SetPredictedIndex(42)
	after := proposal.Data()
	require.Equal(t, before, after[:len(before)])
	require.Equal(t, reserved, cap(proposal.data), "predicted-index patch must not grow the backing buffer")
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
