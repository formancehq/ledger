package node

import (
	"testing"

	"github.com/stretchr/testify/require"

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
