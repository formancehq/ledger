package membership

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// noopTransport / noopPool satisfy Transport / Pool with no
// side effect — Membership tests that don't exercise the wiring side
// pass these so the production code path stays branchless (no nil
// checks).
type noopTransport struct{}

func (noopTransport) AddPeer(uint64, string)             {}
func (noopTransport) RemovePeer(context.Context, uint64) {} //nolint:gochecknoglobals

type noopPool struct{}

func (noopPool) AddPeer(uint64, string) error { return nil }
func (noopPool) RemovePeer(uint64) error      { return nil }

// countingTransport / countingPool count Add / Remove calls. Used to
// verify the Start-gated wiring — pre-Start counts must stay at zero,
// post-Start Set / Remove / Rehydrate must increment.
type countingTransport struct {
	adds    int
	removes int
}

func (c *countingTransport) AddPeer(uint64, string)             { c.adds++ }
func (c *countingTransport) RemovePeer(context.Context, uint64) { c.removes++ }

type countingPool struct {
	adds    int
	removes int
}

func (c *countingPool) AddPeer(uint64, string) error {
	c.adds++

	return nil
}

func (c *countingPool) RemovePeer(uint64) error {
	c.removes++

	return nil
}

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

// testSelfNodeID / testSelfRaftAddr / testSelfServiceAddr identify the
// synthetic "self" used by Membership tests. Kept far away from the
// peer IDs the tests exercise (1, 2, 3, 7, ...) so a `self.NodeID`
// row in Pebble never collides with the peers under assertion.
const (
	testSelfNodeID      = 42
	testSelfRaftAddr    = "self:7777"
	testSelfServiceAddr = "self:8888"
)

// newTestMembership returns a Membership backed by a fresh in-memory
// Pebble store and noop transport/pool, in the post-Start state so
// that Set / Remove / Rehydrate exercise the wire path (against the
// noops). Tests that need the pre-Start behavior should construct
// NewMembership directly.
func newTestMembership(t *testing.T) *Membership {
	t.Helper()

	m, err := NewMembership(newTestPeerStore(t), noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	m.Start()

	return m
}

func TestPeerStore_PutLoadAll(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(1, "pod-0:7777", "pod-0:8888", nil))
	require.NoError(t, ps.Put(2, "pod-1:7777", "pod-1:8888", nil))
	require.NoError(t, ps.Put(3, "pod-2:7777", "pod-2:8888", nil))

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

	require.NoError(t, ps.Put(7, "old:7777", "old:8888", nil))
	require.NoError(t, ps.Put(7, "new:7777", "new:8888", nil))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.Equal(t, "new:7777", peers[7].RaftAddress)
	require.Equal(t, "new:8888", peers[7].ServiceAddress)
}

func TestPeerStore_Delete(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(1, "a:1", "a:2", nil))
	require.NoError(t, ps.Put(2, "b:1", "b:2", nil))
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
// The FSM handler writes ONLY to the supplied batch (no cache/transport
// side effect); the cache update is the responsibility of
// Node.finishReady once the entry is observed post-commit. This test
// therefore pins the three relevant transition types (Add / AddLearner
// / Remove) and the PromoteLearner no-op (empty context) by asserting
// the Pebble write via LoadAll after commit, and asserts that the
// in-memory cache is NOT touched by the handler.
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

	require.Empty(t, m.PeerAddresses(), "FSM handler must not touch the cache; finishReady owns that")

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

	// RemoveNode → peer delete + blacklist write (EN-1045). Every
	// RemoveNode proposal must carry the target's instance_id in
	// Context — WriteConfChange fails loudly otherwise.
	removeCtx, err := MarshalConfChangeContext(ConfChangeContext{InstanceID: make([]byte, 16)})
	require.NoError(t, err)
	apply(t, raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{Type: raftpb.ConfChangeRemoveNode, NodeID: 1}},
		Context: removeCtx,
	}, true)

	got, err = m.store.LoadAll()
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotContains(t, got, uint64(1))
	require.Contains(t, got, uint64(2))

	require.Empty(t, m.PeerAddresses(), "FSM handler must not touch the cache; finishReady owns that")
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
	require.NoError(t, ps.Put(7, "old:1", "old:2", nil))

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)
	m.Start()
	require.Equal(t, "old:1", m.PeerAddresses()[7].RaftAddress)

	// Simulate a leader checkpoint restore: Pebble now has peers 1 + 3,
	// and no peer 7 (the source cluster A has been replaced by cluster B).
	require.NoError(t, ps.Delete(7))
	require.NoError(t, ps.Put(1, "new:1", "new:2", nil))
	require.NoError(t, ps.Put(3, "new:3", "new:4", nil))

	// Hook fires: cache must catch up to the new Pebble state.
	m.OnSnapshotInstalled()

	got := m.PeerAddresses()
	// Cache holds peers 1 + 3 from the leader's checkpoint, plus self
	// which OnSnapshotInstalled re-upserts to the locally-known truth.
	require.Len(t, got, 3, "stale peer 7 must be gone, peers 1+3 + self must be present")
	require.Equal(t, "new:1", got[1].RaftAddress)
	require.Equal(t, "new:3", got[3].RaftAddress)
	require.Equal(t, testSelfRaftAddr, got[testSelfNodeID].RaftAddress, "self must be re-upserted with local truth")
	require.NotContains(t, got, uint64(7))
}

// TestMembership_RehydrateAfterReplay covers the EN-1413 follow-up gap:
// WAL replay applies ConfChange entries to Pebble via WriteConfChange
// (FSM hot path) but does NOT touch the in-memory cache — finishReady,
// which owns that side effect on the normal Ready path, never runs for
// replayed entries. NewNode therefore calls Rehydrate after
// RecoverAndReplay so the recovered cache + transport match Pebble
// before the Raft node starts.
//
// This simulates the scenario: cache loaded at boot from Pebble (peer
// 7), then "WAL replay" mutates Pebble out-of-band (peer 7 removed,
// peers 1 and 3 added), then Rehydrate must catch the cache up.
func TestMembership_RehydrateAfterReplay(t *testing.T) {
	t.Parallel()

	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(7, "before:1", "before:2", nil))

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)
	m.Start()
	require.Equal(t, "before:1", m.PeerAddresses()[7].RaftAddress)

	// Simulate WAL replay: WriteConfChange wrote these rows directly to
	// Pebble without touching the cache.
	require.NoError(t, ps.Delete(7))
	require.NoError(t, ps.Put(1, "after:1", "after:2", nil))
	require.NoError(t, ps.Put(3, "after:3", "after:4", nil))

	require.NoError(t, m.Rehydrate())

	got := m.PeerAddresses()
	require.Len(t, got, 2, "replayed removal of peer 7 + additions of 1 and 3 must reflect in cache")
	require.Equal(t, "after:1", got[1].RaftAddress)
	require.Equal(t, "after:3", got[3].RaftAddress)
	require.NotContains(t, got, uint64(7))
}

// TestMembership_StartGate pins the Start-gated wiring behavior: any
// Set / Remove / Rehydrate that fires BEFORE Start mutates only the
// cache; the transport + service pool see no calls. Once Start fires,
// the current cache is flushed to the transport + pool, and subsequent
// Set / Remove wire inline. This gate exists because the initial
// construction runs during Fx wiring, before the local Raft gRPC
// server is listening — a pool.AddPeer in that window would probe a
// non-listening remote peer under --tls-mode=optional and fail
// permanently.
func TestMembership_StartGate(t *testing.T) {
	t.Parallel()

	transport := &countingTransport{}
	pool := &countingPool{}
	ps := newTestPeerStore(t)

	require.NoError(t, ps.Put(1, "pod-0:7777", "pod-0:8888", nil))

	m, err := NewMembership(ps, transport, pool, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	require.Equal(t, 0, transport.adds, "no wire before Start (cache-only construction)")
	require.Equal(t, 0, pool.adds, "no wire before Start (cache-only construction)")

	m.Set(2, "pod-1:7777", "pod-1:8888", nil)
	require.Equal(t, 0, transport.adds, "Set pre-Start must be cache-only")
	require.Equal(t, 0, pool.adds, "Set pre-Start must be cache-only")
	require.Len(t, m.PeerAddresses(), 2, "cache must still reflect the Set")

	m.Start()
	require.Equal(t, 2, transport.adds, "Start flushes the cache to the transport")
	require.Equal(t, 2, pool.adds, "Start flushes the cache to the service pool")

	m.Set(3, "pod-2:7777", "pod-2:8888", nil)
	require.Equal(t, 3, transport.adds, "Set post-Start wires inline")
	require.Equal(t, 3, pool.adds, "Set post-Start wires inline")

	m.Remove(1)
	require.Equal(t, 1, transport.removes, "Remove post-Start wires inline")
	require.Equal(t, 1, pool.removes, "Remove post-Start wires inline")
}

// TestMembership_ReconcileAgainstConfState pins the boot-time cleanup
// pass that drops peers whose NodeID is no longer in the durable
// ConfState. This covers two crash windows: interrupted ForceRemoveNode
// (ConfState updated, Unregister not committed) and a restored backup
// carrying source-cluster peers without matching ConfState entries.
// Self is always kept, learners are honored, an empty ConfState clears
// everything except self, and a repeat call is a no-op.
func TestMembership_ReconcileAgainstConfState(t *testing.T) {
	t.Parallel()

	newSeeded := func(t *testing.T) *Membership {
		ps := newTestPeerStore(t)
		require.NoError(t, ps.Put(7, "self:1", "self:2", nil))
		require.NoError(t, ps.Put(1, "voter1:1", "voter1:2", nil))
		require.NoError(t, ps.Put(2, "voter2:1", "voter2:2", nil))
		require.NoError(t, ps.Put(3, "learner:1", "learner:2", nil))
		require.NoError(t, ps.Put(99, "stale:1", "stale:2", nil))

		m, err := NewMembership(ps, noopTransport{}, noopPool{}, 7, "self:1", "self:2", nil, logging.Testing())
		require.NoError(t, err)
		m.Start()

		return m
	}

	t.Run("drops stale voter", func(t *testing.T) {
		t.Parallel()
		m := newSeeded(t)

		require.NoError(t, m.ReconcileAgainstConfState(raftpb.ConfState{
			Voters:   []uint64{1, 2, 7},
			Learners: []uint64{3},
		}))

		got := m.PeerAddresses()
		require.NotContains(t, got, uint64(99), "peer not in ConfState must be dropped")
		require.Contains(t, got, uint64(1))
		require.Contains(t, got, uint64(2))
		require.Contains(t, got, uint64(3), "learner must be kept")
		require.Contains(t, got, uint64(7), "self must be kept")
	})

	t.Run("keeps self even when not in ConfState", func(t *testing.T) {
		t.Parallel()
		m := newSeeded(t)

		require.NoError(t, m.ReconcileAgainstConfState(raftpb.ConfState{
			Voters: []uint64{1, 2},
		}))

		require.Contains(t, m.PeerAddresses(), uint64(7), "self must never be dropped by reconcile")
	})

	t.Run("empty ConfState keeps only self", func(t *testing.T) {
		t.Parallel()
		m := newSeeded(t)

		require.NoError(t, m.ReconcileAgainstConfState(raftpb.ConfState{}))

		got := m.PeerAddresses()
		require.Len(t, got, 1, "empty ConfState must clear everything except self")
		require.Contains(t, got, uint64(7))
	})

	t.Run("idempotent second call", func(t *testing.T) {
		t.Parallel()
		m := newSeeded(t)

		cs := raftpb.ConfState{Voters: []uint64{1, 2, 7}, Learners: []uint64{3}}
		require.NoError(t, m.ReconcileAgainstConfState(cs))
		before := m.PeerAddresses()

		require.NoError(t, m.ReconcileAgainstConfState(cs))
		require.Equal(t, before, m.PeerAddresses(), "second call must be a no-op")
	})
}

// TestWalkConfChangeContexts_MultiAddInvariant pins the invariant #7
// enforcement: a ConfChangeV2 that bundles multiple Add / AddLearner
// changes with a non-empty Context is an impossible-by-design shape
// (all local propose paths emit single-Add batches; joint consensus
// isn't used), and cc.Context can only address a single peer. Silently
// degrading (dropping the address for every added node) would leave
// voters recorded in ConfState with no dialable Pebble row — a
// downstream corruption that's hard to trace. Fail loudly instead.
func TestWalkConfChangeContexts_MultiAddInvariant(t *testing.T) {
	t.Parallel()

	ctx, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    "pod-1:7777",
		ServiceAddress: "pod-1:8888",
	})
	require.NoError(t, err)

	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{Type: raftpb.ConfChangeAddNode, NodeID: 1},
			{Type: raftpb.ConfChangeAddNode, NodeID: 2},
		},
		Context: ctx,
	}

	callCount := 0
	err = WalkConfChangeContexts(cc, func(raftpb.ConfChangeType, uint64, *ConfChangeContext) error {
		callCount++

		return nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invariant:")
	require.Zero(t, callCount, "fn must not fire when the invariant is violated")
}

// Multi-Remove is fine — cc.Context is empty for RemoveNode and there's
// no per-peer address ambiguity, so bundling removes is allowed.
func TestWalkConfChangeContexts_MultiRemoveAllowed(t *testing.T) {
	t.Parallel()

	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{
			{Type: raftpb.ConfChangeRemoveNode, NodeID: 1},
			{Type: raftpb.ConfChangeRemoveNode, NodeID: 2},
		},
	}

	var seen []uint64
	err := WalkConfChangeContexts(cc, func(_ raftpb.ConfChangeType, nodeID uint64, ctx *ConfChangeContext) error {
		require.Nil(t, ctx)
		seen = append(seen, nodeID)

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, seen)
}
