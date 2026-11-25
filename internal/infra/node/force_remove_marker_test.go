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
	require.NoError(t, writeForceRemoveMarker(dir, 3, identity, 42))

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

func TestRecoverForceRemoveMarkerPreservesTimestampOnReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	peers := newTestMembership(t)
	identity := []byte("0000000000000003")
	require.NoError(t, peers.Register(3, "node-3:7000", "node-3:8000", identity, nil))
	require.NoError(t, writeForceRemoveMarker(dir, 3, identity, 42))

	removedConfState := &raftpb.ConfState{Voters: []uint64{1, 2}}
	require.NoError(t, recoverForceRemoveMarker(dir, removedConfState, peers))
	removed, err := peers.PeerStore().LoadAllRemoved()
	require.NoError(t, err)
	require.Len(t, removed, 1)
	require.Equal(t, uint64(42), removed[0].GetRemovedAt())

	// A failed marker clear leaves a marker behind after the Pebble batch.
	// Replaying that marker must retain the original timestamp.
	require.NoError(t, writeForceRemoveMarker(dir, 3, identity, 42))
	require.NoError(t, recoverForceRemoveMarker(dir, removedConfState, peers))
	removed, err = peers.PeerStore().LoadAllRemoved()
	require.NoError(t, err)
	require.Len(t, removed, 1)
	require.Equal(t, uint64(42), removed[0].GetRemovedAt())
}
