package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestRecoverForceRemoveMarkerRetainsMarkerWhenPebbleSyncFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	identity := []byte("0000000000000003")
	injected := errors.New("injected Pebble sync failure")
	failSync := true
	cfg := dal.DefaultConfig()
	cfg.BeforeWALSync = func() error {
		if failSync {
			return injected
		}

		return nil
	}
	store, err := dal.NewStore(t.TempDir(), logging.Testing(),
		noop.NewMeterProvider().Meter("force-remove-sync"), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	peers, err := membership.NewMembership(membership.NewPeerStore(store),
		noopMemTransport{}, noopMemPool{}, 42, "self:7777", "self:8888",
		[]byte("self-instance-id"), logging.Testing(), membership.NoopClusterPolicyProvider)
	require.NoError(t, err)
	require.NoError(t, peers.Register(3, "node-3:7000", "node-3:8000", identity, nil))
	require.NoError(t, writeForceRemoveMarker(dir, 3, identity, 42))
	removedConfState := &raftpb.ConfState{Voters: []uint64{1, 2}}

	err = recoverForceRemoveMarker(dir, removedConfState, peers)
	require.ErrorIs(t, err, injected)
	marker, err := readForceRemoveMarker(dir)
	require.NoError(t, err)
	require.NotNil(t, marker)
	removed, err := peers.IsRemoved(3, identity)
	require.NoError(t, err)
	require.True(t, removed, "the batch committed before the sync failure")

	failSync = false
	require.NoError(t, recoverForceRemoveMarker(dir, removedConfState, peers))
	marker, err = readForceRemoveMarker(dir)
	require.NoError(t, err)
	require.Nil(t, marker)
}

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
