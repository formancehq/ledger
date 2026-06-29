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
