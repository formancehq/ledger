package raftcmdpb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProposalAtomicTechnicalUpdatesWireCompatibility(t *testing.T) {
	t.Parallel()
	// Literal historical bytes: an empty Proposal and field 10 varint true.
	// Default false must not change historical wire encoding.
	for _, tc := range []struct {
		name string
		wire []byte
		want bool
	}{
		{name: "historical empty"},
		{name: "explicit false", wire: []byte{0x50, 0x00}},
		{name: "marked", wire: []byte{0x50, 0x01}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &Proposal{}
			require.NoError(t, p.UnmarshalVT(tc.wire))
			require.Equal(t, tc.want, p.GetAtomicTechnicalUpdates())
			require.Equal(t, tc.want, p.CloneVT().GetAtomicTechnicalUpdates())
			canonical := []byte{}
			if tc.want {
				canonical = []byte{0x50, 0x01}
			}
			vt, err := p.MarshalVT()
			require.NoError(t, err)
			require.True(t, bytes.Equal(canonical, vt))
			standard, err := proto.Marshal(p)
			require.NoError(t, err)
			require.True(t, bytes.Equal(canonical, standard))
		})
	}
}
