package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
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

// newTestMembership returns a Membership backed by a fresh in-memory
// Pebble store. Transport and pool are nil — Membership tolerates this
// for tests that don't exercise the wiring side. Used to seed a minimal
// Node in tests that only touch peer state.
func newTestMembership(t *testing.T) *Membership {
	t.Helper()

	m, err := NewMembership(newTestPeerStore(t), nil, nil, 0, logging.Testing())
	require.NoError(t, err)

	return m
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

// TestMembership_WriteConfChange covers the EN-1413 follow-up fix for
// the "ConfChange lost across snapshot install" scenario: the FSM, in
// PrepareEntries, invokes this handler for every EntryConfChange* with
// the in-flight WriteSession. The peer mutation lands in the same
// Pebble batch as the surrounding business writes — atomic, idempotent
// across spool/WAL replay.
//
// This test pins the three relevant transition types (Add / AddLearner
// / Remove), the PromoteLearner no-op (empty context), and asserts both
// the Pebble write (via LoadAll after commit) and the in-memory cache
// update.
func TestMembership_WriteConfChange(t *testing.T) {
	t.Parallel()

	m := newTestMembership(t)

	addCtx, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "pod-1:7777",
		ServiceAddress: "pod-1:8888",
	})
	require.NoError(t, err)

	addLearnerCtx, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "pod-2:7777",
		ServiceAddress: "pod-2:8888",
	})
	require.NoError(t, err)

	// apply runs the handler on a fresh WriteSession + commits it, so
	// the test exercises the same Pebble write path the FSM uses.
	apply := func(t *testing.T, cc raftpb.ConfChangeV2, v2 bool) {
		t.Helper()

		data, err := cc.Marshal()
		require.NoError(t, err)

		entry := raftpb.Entry{
			Type: raftpb.EntryConfChangeV2,
			Data: data,
		}
		if !v2 {
			ccV1 := raftpb.ConfChange{
				Type:    cc.Changes[0].Type,
				NodeID:  cc.Changes[0].NodeID,
				Context: cc.Context,
			}
			d, err := ccV1.Marshal()
			require.NoError(t, err)

			entry = raftpb.Entry{Type: raftpb.EntryConfChange, Data: d}
		}

		session := m.store.store.OpenWriteSession()
		require.NoError(t, m.WriteConfChange(entry, session))
		require.NoError(t, session.Commit())
	}

	apply(t, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 1}},
		Context: addCtx,
	}, false)
	apply(t, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddLearnerNode, NodeID: 2}},
		Context: addLearnerCtx,
	}, true)

	got, err := m.store.LoadAll()
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "pod-1:7777", got[1].RaftAddress)
	require.Equal(t, "pod-2:7777", got[2].RaftAddress)

	require.Len(t, m.PeerAddresses(), 2, "cache must mirror the Pebble write")

	// PromoteLearner (ConfChangeAddNode without context) — must be a
	// no-op for the peer payload: it's a role change, not an address
	// change.
	apply(t, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeAddNode, NodeID: 2}},
		Context: nil,
	}, true)

	got, err = m.store.LoadAll()
	require.NoError(t, err)
	require.Equal(t, "pod-2:7777", got[2].RaftAddress, "promote must not overwrite the address")

	// RemoveNode → Pebble delete + cache delete.
	apply(t, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 1}},
	}, true)

	got, err = m.store.LoadAll()
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotContains(t, got, uint64(1))
	require.Contains(t, got, uint64(2))

	cache := m.PeerAddresses()
	require.NotContains(t, cache, uint64(1), "cache must drop the removed peer")
	require.Contains(t, cache, uint64(2))
}

// TestMembership_OnSnapshotInstalled exercises the EN-1413 async-
// snapshot-install fix. The Applier's OnSnapshotInstalled hook fires
// AFTER the leader's Pebble checkpoint has been swapped in. This test
// simulates that swap by mutating Pebble out-of-band, then invokes the
// hook and asserts the in-memory peer-address cache picks up the new
// contents.
func TestMembership_OnSnapshotInstalled(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	// Pre-swap state: cluster A had peer 7.
	require.NoError(t, ps.Put(7, "old:1", "old:2"))

	m, err := NewMembership(ps, nil, nil, 0, logging.Testing())
	require.NoError(t, err)
	require.Equal(t, "old:1", m.PeerAddresses()[7].RaftAddress)

	// Simulate a leader checkpoint restore: Pebble now has peers 1 + 3,
	// and no peer 7 (the source cluster A has been replaced by cluster B).
	require.NoError(t, ps.Delete(7))
	require.NoError(t, ps.Put(1, "new:1", "new:2"))
	require.NoError(t, ps.Put(3, "new:3", "new:4"))

	// Hook fires: cache must catch up to the new Pebble state.
	m.OnSnapshotInstalled()

	got := m.PeerAddresses()
	require.Len(t, got, 2, "stale peer 7 must be gone, peers 1+3 must be present")
	require.Equal(t, "new:1", got[1].RaftAddress)
	require.Equal(t, "new:3", got[3].RaftAddress)
	require.NotContains(t, got, uint64(7))
}
