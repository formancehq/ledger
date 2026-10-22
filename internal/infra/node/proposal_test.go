package node

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestNewPreparedProposalCopiesSerializedData(t *testing.T) {
	prepared, err := capabilities.PrepareProposal(&raftcmdpb.Proposal{})
	require.NoError(t, err)

	input := []byte{1, 2, 3}
	proposal := NewPreparedProposal(1, input, prepared)
	input[0] = 9
	require.Equal(t, []byte{1, 2, 3}, proposal.Data())

	data := proposal.Data()
	data[1] = 9
	require.Equal(t, []byte{1, 2, 3}, proposal.Data())
}
