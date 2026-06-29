package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newTestPeerStore returns a fresh PeerStore backed by an in-memory Pebble
// store in a temp directory, cleaned up at test end.
func newTestPeerStore(t *testing.T) *PeerStore {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return NewPeerStore(store)
}

func TestPeerStore_PutLoadAll(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(1, "pod-0:7777", "pod-0:8888"))
	require.NoError(t, ps.Put(2, "pod-1:7777", "pod-1:8888"))
	require.NoError(t, ps.Put(3, "pod-2:7777", "pod-2:8888"))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.Len(t, peers, 3)
	require.Equal(t, ConfChangeContext{RaftAddress: "pod-0:7777", ServiceAddress: "pod-0:8888"}, peers[1])
	require.Equal(t, ConfChangeContext{RaftAddress: "pod-1:7777", ServiceAddress: "pod-1:8888"}, peers[2])
	require.Equal(t, ConfChangeContext{RaftAddress: "pod-2:7777", ServiceAddress: "pod-2:8888"}, peers[3])
}

// TestPeerStore_PutOverwrites verifies that re-Putting the same NodeID
// replaces the previous entry (UpdateNode semantics: a peer that changes
// address gets the new value, not a duplicate).
func TestPeerStore_PutOverwrites(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(7, "old:7777", "old:8888"))
	require.NoError(t, ps.Put(7, "new:7777", "new:8888"))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "new:7777", peers[7].RaftAddress)
	require.Equal(t, "new:8888", peers[7].ServiceAddress)
}

func TestPeerStore_Delete(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(1, "a:1", "a:2"))
	require.NoError(t, ps.Put(2, "b:1", "b:2"))
	require.NoError(t, ps.Delete(1))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.NotContains(t, peers, uint64(1))
	require.Contains(t, peers, uint64(2))
}

// TestPeerStore_DeleteAbsentIsNoop verifies that Delete on a non-existent
// NodeID does not error. ConfChange RemoveNode replays are legal in raft
// (duplicate apply), and a hard error would crash the FSM apply path.
func TestPeerStore_DeleteAbsentIsNoop(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Delete(42))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.Empty(t, peers)
}

// TestPeerStore_LoadAllEmpty is the cluster-first-boot case: no Put has
// run yet, LoadAll must return an empty map (not nil and not an error).
// NewNode relies on this to seed a non-nil node.peerAddresses cache.
func TestPeerStore_LoadAllEmpty(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.NotNil(t, peers)
	require.Empty(t, peers)
}

// TestPeerStore_KeyEncodingIsScoped pins the key layout to
// [ZoneGlobal][SubGlobPeers][BE 8] so a future global-prefix addition (or
// a stray Put with a colliding sub-prefix) is caught by the test instead
// of silently entangling with peer iteration.
func TestPeerStore_KeyEncodingIsScoped(t *testing.T) {
	t.Parallel()

	k := peerKey(0x0102030405060708)
	require.Equal(t, []byte{
		dal.ZoneGlobal,
		dal.SubGlobPeers,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	}, k)
}

// TestNode_reloadPeersFromStore_RefreshesCache exercises the EN-1413
// async-snapshot-install fix. The Applier's OnSnapshotInstalled hook
// fires AFTER the leader's Pebble checkpoint has been swapped in. This
// test simulates that swap by mutating Pebble out-of-band, then invokes
// the hook and asserts the in-memory peer-address cache picks up the
// new contents.
//
// Without the fix, the inline LoadAll in processReady would have run
// before the swap and the cache would stay at its pre-restore state.
// The hook is the only thing that guarantees the cache reflects the
// restored Pebble.
func TestNode_reloadPeersFromStore_RefreshesCache(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	// Pre-swap state: cluster A had peer 7.
	require.NoError(t, ps.Put(7, "old:1", "old:2"))

	// Build a minimal Node with just the fields reloadPeersFromStore
	// touches. The real wiring goes through NewNode + bootstrap, but
	// that requires a Raft loop + WAL we don't need to exercise here.
	n := &Node{
		logger:        logging.Testing(),
		peerStore:     ps,
		peerAddresses: map[uint64]ConfChangeContext{7: {RaftAddress: "old:1", ServiceAddress: "old:2"}},
	}

	// Simulate a leader checkpoint restore: Pebble now has peers 1 + 3,
	// and no peer 7 (the source cluster A has been replaced by cluster B).
	require.NoError(t, ps.Delete(7))
	require.NoError(t, ps.Put(1, "new:1", "new:2"))
	require.NoError(t, ps.Put(3, "new:3", "new:4"))

	// Hook fires: cache must catch up to the new Pebble state.
	n.reloadPeersFromStore()

	got := n.PeerAddresses()
	require.Len(t, got, 2, "stale peer 7 must be gone, peers 1+3 must be present")
	require.Equal(t, "new:1", got[1].RaftAddress)
	require.Equal(t, "new:3", got[3].RaftAddress)
	require.NotContains(t, got, uint64(7))
}
