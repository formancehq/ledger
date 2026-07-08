// End-to-end tests for the removed-member registry at the membership
// layer: propose → FSM apply → registry, and force-remove atomic batch.
// Verifies the invariants the design doc promises without spinning up a
// full cluster.
package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// storesForBlacklistTest returns a PeerStore backed by a fresh in-memory
// Pebble DAL, so tests exercise the actual atomic batch semantics of
// peer-row delete + blacklist write rather than a mocked-out storage layer.
func storesForBlacklistTest(t *testing.T) (*PeerStore, *dal.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("blacklist-test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return NewPeerStore(store), store
}

func fixedInstanceID(pattern byte) []byte {
	out := make([]byte, 16)
	for i := range out {
		out[i] = pattern
	}

	return out
}

// TestWriteConfChange_RemoveNodeBlacklistsPeer covers the consensus path:
// a ConfChangeRemoveNode carrying an instance_id in its context lands the
// peer row delete AND the RemovedMemberEntry in the same Pebble batch.
func TestWriteConfChange_RemoveNodeBlacklistsPeer(t *testing.T) {
	t.Parallel()

	ps, store := storesForBlacklistTest(t)
	instanceID := fixedInstanceID(0xAA)

	require.NoError(t, ps.Put(3, "pod-2:7777", "pod-2:8888", instanceID))

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	ccCtx, err := MarshalConfChangeContext(ConfChangeContext{InstanceID: instanceID})
	require.NoError(t, err)

	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 3,
		}},
		Context: ccCtx,
	}
	data, err := cc.Marshal()
	require.NoError(t, err)

	entry := raftpb.Entry{Type: raftpb.EntryConfChangeV2, Data: data}

	session := store.OpenWriteSession()
	require.NoError(t, m.WriteConfChange(entry, session))
	require.NoError(t, session.Commit())

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.NotContains(t, peers, uint64(3))

	hit, err := ps.IsRemoved(3, instanceID)
	require.NoError(t, err)
	require.True(t, hit, "consensus RemoveNode must land a blacklist entry atomically with the peer row delete")
}

// TestWriteConfChange_RemoveNodeWithoutContextSkipsBlacklist covers the
// phantom-learner path: a peer added via the admin cluster.AddLearner RPC
// without the target ever booting has an empty instance_id in its
// Membership row. RemoveNode on such a peer proposes without a Context;
// FSM apply must delete the peer row and skip the blacklist entry (there
// is nothing to blacklist).
func TestWriteConfChange_RemoveNodeWithoutContextSkipsBlacklist(t *testing.T) {
	t.Parallel()

	ps, store := storesForBlacklistTest(t)

	// Phantom learner: row exists, no instance_id.
	require.NoError(t, ps.Put(3, "pod-2:7777", "pod-2:8888", nil))

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	cc := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: 3,
		}},
	}
	data, err := cc.Marshal()
	require.NoError(t, err)

	entry := raftpb.Entry{Type: raftpb.EntryConfChangeV2, Data: data}

	session := store.OpenWriteSession()
	require.NoError(t, m.WriteConfChange(entry, session))
	require.NoError(t, session.Commit())

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.NotContains(t, peers, uint64(3))

	dumped, err := ps.LoadAllRemoved()
	require.NoError(t, err)
	require.Empty(t, dumped, "phantom-learner removal must not create a blacklist entry")
}

// TestUnregisterAndBlacklist_AtomicBatch pins the force-remove semantics:
// peer row delete AND blacklist write commit together.
func TestUnregisterAndBlacklist_AtomicBatch(t *testing.T) {
	t.Parallel()

	ps, _ := storesForBlacklistTest(t)
	instanceID := fixedInstanceID(0xBB)

	require.NoError(t, ps.Put(7, "pod-6:7777", "pod-6:8888", instanceID))

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	require.NoError(t, m.UnregisterAndBlacklist(7, instanceID, 42))

	peers, err := ps.LoadAll()
	require.NoError(t, err)
	require.NotContains(t, peers, uint64(7))

	hit, err := ps.IsRemoved(7, instanceID)
	require.NoError(t, err)
	require.True(t, hit)
}

// TestUnregisterAndBlacklist_RejectsWrongInstanceIDLen pins the contract:
// the identity we blacklist on must be exactly 16 bytes.
func TestUnregisterAndBlacklist_RejectsWrongInstanceIDLen(t *testing.T) {
	t.Parallel()

	ps, _ := storesForBlacklistTest(t)

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	require.Error(t, m.UnregisterAndBlacklist(7, []byte("short"), 0))
}

// TestIsRemoved_MatchesOnExactTuple verifies the discrimination the whole
// EN-1045 design hinges on: same (nodeID, instanceID) hits, same nodeID
// with a different instanceID misses (fresh-pod-at-reused-ordinal case).
func TestIsRemoved_MatchesOnExactTuple(t *testing.T) {
	t.Parallel()

	ps, _ := storesForBlacklistTest(t)
	blacklisted := fixedInstanceID(0xCC)

	m, err := NewMembership(ps, noopTransport{}, noopPool{}, testSelfNodeID, testSelfRaftAddr, testSelfServiceAddr, nil, logging.Testing())
	require.NoError(t, err)

	require.NoError(t, m.UnregisterAndBlacklist(9, blacklisted, 100))

	hit, err := m.IsRemoved(9, blacklisted)
	require.NoError(t, err)
	require.True(t, hit)

	fresh, err := m.IsRemoved(9, fixedInstanceID(0xDD))
	require.NoError(t, err)
	require.False(t, fresh, "a fresh pod at the same nodeID with a different instance_id must not be blacklisted")
}
