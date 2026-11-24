package raftcmdpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProposalUnmarshalRejectsMalformedPackedCapabilities(t *testing.T) {
	t.Parallel()
	// The fixed32 payload is one byte; trailing bytes belong to another field.
	malformed := []byte{0x52, 0x01, 0x01, 0x10, 0x80, 0x01}
	require.Error(t, new(Proposal).UnmarshalVT(malformed))

	valid := []byte{0x52, 0x04, 0x07, 0x00, 0x00, 0x00}
	proposal := new(Proposal)
	require.NoError(t, proposal.UnmarshalVT(valid))
	require.Equal(t, []uint32{7}, proposal.GetRequiredCapabilities())
}
