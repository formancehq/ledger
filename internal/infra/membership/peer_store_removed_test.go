package membership

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newTestPeerStoreWithDAL(t *testing.T) (*PeerStore, *dal.Store) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return NewPeerStore(store), store
}

// bytesN returns a 16-byte pattern-filled slice, deterministic across runs
// so failing assertions produce readable diffs.
func bytesN(pattern byte) []byte {
	out := make([]byte, 16)
	for i := range out {
		out[i] = pattern
	}

	return out
}

func TestPeerStore_IsRemovedMissingReturnsFalse(t *testing.T) {
	t.Parallel()

	rms, _ := newTestPeerStoreWithDAL(t)

	got, err := rms.IsRemoved(3, bytesN(0xAA))
	require.NoError(t, err)
	require.False(t, got)
}

func TestPeerStore_IsRemovedRejectsEmptyInstanceID(t *testing.T) {
	t.Parallel()

	rms, _ := newTestPeerStoreWithDAL(t)

	// Empty instance id is an invariant violation: every cluster member
	// acquires one at first boot via wal.EnsureInstanceID.
	_, err := rms.IsRemoved(3, nil)
	require.Error(t, err)
}

func TestPeerStore_MarkRemovedThenIsRemoved(t *testing.T) {
	t.Parallel()

	rms, store := newTestPeerStoreWithDAL(t)

	session := store.OpenWriteSession()
	require.NoError(t, rms.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
		NodeId:     3,
		InstanceId: bytesN(0xAA),
		Reason:     "consensus",
	}))
	require.NoError(t, session.Commit())

	got, err := rms.IsRemoved(3, bytesN(0xAA))
	require.NoError(t, err)
	require.True(t, got, "MarkRemoved+Commit must make the entry visible to Contains")
}

// TestPeerStore_InstanceIDDiscriminates pins the EN-1045 guarantee:
// the same nodeID with a DIFFERENT instanceID must not be blacklisted.
// This is the case that distinguishes a still-alive removed pod (same
// instance_id) from a fresh pod at the reused ordinal (new instance_id).
func TestPeerStore_InstanceIDDiscriminates(t *testing.T) {
	t.Parallel()

	rms, store := newTestPeerStoreWithDAL(t)

	session := store.OpenWriteSession()
	require.NoError(t, rms.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
		NodeId:     3,
		InstanceId: bytesN(0xAA),
		Reason:     "consensus",
	}))
	require.NoError(t, session.Commit())

	hit, err := rms.IsRemoved(3, bytesN(0xAA))
	require.NoError(t, err)
	require.True(t, hit, "same (nodeID, instanceID) → blacklisted")

	miss, err := rms.IsRemoved(3, bytesN(0xBB))
	require.NoError(t, err)
	require.False(t, miss, "same nodeID with a fresh instance_id → not blacklisted; this is the fresh-pod-at-reused-ordinal case")
}

func TestPeerStore_LoadAll(t *testing.T) {
	t.Parallel()

	rms, store := newTestPeerStoreWithDAL(t)

	entries := []*raftcmdpb.RemovedMemberEntry{
		{NodeId: 3, InstanceId: bytesN(0xAA), Reason: "consensus"},
		{NodeId: 3, InstanceId: bytesN(0xBB), Reason: "force"},
		{NodeId: 5, InstanceId: bytesN(0xCC), Reason: "consensus"},
	}

	session := store.OpenWriteSession()
	for _, e := range entries {
		require.NoError(t, rms.MarkRemoved(session, e))
	}
	require.NoError(t, session.Commit())

	got, err := rms.LoadAllRemoved()
	require.NoError(t, err)
	require.Len(t, got, len(entries))
}

func TestPeerStore_AnyRemovedForNodeID(t *testing.T) {
	t.Parallel()

	rms, store := newTestPeerStoreWithDAL(t)

	session := store.OpenWriteSession()
	require.NoError(t, rms.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
		NodeId:     3,
		InstanceId: bytesN(0xAA),
		Reason:     "consensus",
	}))
	require.NoError(t, session.Commit())

	yes, err := rms.AnyRemovedForNodeID(3)
	require.NoError(t, err)
	require.True(t, yes)

	no, err := rms.AnyRemovedForNodeID(4)
	require.NoError(t, err)
	require.False(t, no)
}

func TestPeerStore_MarkRemovedRejectsWrongInstanceIDLen(t *testing.T) {
	t.Parallel()

	rms, store := newTestPeerStoreWithDAL(t)
	session := store.OpenWriteSession()
	defer func() { _ = session.Cancel() }()

	require.Error(t, rms.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
		NodeId:     3,
		InstanceId: []byte("short"),
		Reason:     "consensus",
	}))
}

func TestPeerStore_IsRemovedRejectsWrongInstanceIDLen(t *testing.T) {
	t.Parallel()

	rms, _ := newTestPeerStoreWithDAL(t)

	_, err := rms.IsRemoved(3, []byte("short-but-nonempty"))
	require.Error(t, err)
}
