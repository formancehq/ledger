package raftcmdpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAttributeValueWireCompatibility(t *testing.T) {
	t.Parallel()
	// Field 1 is the original live payload. A decoded old plan remains live.
	var old AttributeValue
	require.NoError(t, proto.Unmarshal([]byte{0x0a, 0x01, 0x2a}, &old))
	require.False(t, old.GetDeleted())
	require.Equal(t, []byte{0x2a}, old.GetRawValue())

	for _, value := range []*AttributeValue{
		{RawValue: nil},
		{Deleted: true},
	} {
		wire, err := value.MarshalVT()
		require.NoError(t, err)
		var decoded AttributeValue
		require.NoError(t, decoded.UnmarshalVT(wire))
		require.Equal(t, value.GetDeleted(), decoded.GetDeleted())
		require.Equal(t, value.GetRawValue(), decoded.GetRawValue())
		again, err := decoded.MarshalVT()
		require.NoError(t, err)
		require.Equal(t, wire, again)
	}
	live := (&AttributeValue{}).MarshalDeterministicVT(nil)
	deleted := (&AttributeValue{Deleted: true}).MarshalDeterministicVT(nil)
	require.NotEqual(t, live, deleted, "proposal hashing must distinguish a tombstone from a live empty value")
}
