package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRecoverForceRemoveMarkerKeepsMemberWhenConfStateWasNotPersisted(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	peers := newTestMembership(t)
	identity := []byte("0000000000000003")
	require.NoError(t, peers.Register(3, "node-3:7000", "node-3:8000", identity, nil))
	require.NoError(t, writeForceRemoveMarker(dir, 3, identity))

	require.NoError(t, recoverForceRemoveMarker(dir, &raftpb.ConfState{
		Voters: []uint64{1, 2, 3},
	}, peers))
	require.Contains(t, peers.PeerAddresses(), uint64(3))
	removed, err := peers.IsRemoved(3, identity)
	require.NoError(t, err)
	require.False(t, removed)
	marker, err := readForceRemoveMarker(dir)
	require.NoError(t, err)
	require.Nil(t, marker)
}
