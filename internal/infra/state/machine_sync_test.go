package state

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// copyDirFetcher is a test SnapshotFetcher that copies a local directory into
// the target directory provided by the machine (simulating a real peer fetch).
type copyDirFetcher struct {
	srcDir string
}

func (f *copyDirFetcher) FetchSnapshot(_ context.Context, _ uint64, targetDir string, _ *SyncProgress) (uint64, string, error) {
	if err := os.CopyFS(targetDir, os.DirFS(f.srcDir)); err != nil {
		return 0, "", err
	}

	return 0, "", nil
}

// TestSynchronizeWithLeaderAlwaysFetches is a regression test for the
// checkpointId collision bug: when a follower's local currentCheckpointID
// equals the leader's checkpointId in the Raft snapshot, the old code would
// skip fetching the checkpoint (guard: currentCheckpointID < lastCheckpointID).
// But checkpointIds are per-node monotonic counters — identical values across
// nodes can correspond to Pebble dumps taken at different Raft indices (Ready
// batching is not synchronized across nodes). The fix always fetches.
func TestSynchronizeWithLeaderAlwaysFetches(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	meter := noop.NewMeterProvider().Meter("test")
	logger := logging.FromContext(logging.TestingContext())

	// --- Leader store: "leader-ledger" at Raft index 100, checkpoint 1 ---
	leaderStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = leaderStore.Close() })

	registerLedger(t, leaderStore, "leader-ledger")

	leaderBatch := leaderStore.NewBatch()
	require.NoError(t, SetAppliedIndex(leaderBatch, 100))
	require.NoError(t, leaderBatch.Commit())

	leaderCheckpointID, err := leaderStore.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), leaderCheckpointID)

	leaderCheckpointPath, err := leaderStore.GetCheckpointPath(1)
	require.NoError(t, err)

	// --- Follower machine: "follower-ledger" at Raft index 90, checkpoint 1 ---
	// Both nodes happen to assign checkpointId=1 to snapshots taken at
	// different Raft indices — the classic collision scenario.
	followerMachine, followerStore, _ := newTestMachine(t)

	registerLedger(t, followerStore, "follower-ledger")

	followerBatch := followerStore.NewBatch()
	require.NoError(t, SetAppliedIndex(followerBatch, 90))
	require.NoError(t, followerBatch.Commit())

	followerCheckpointID, err := followerStore.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), followerCheckpointID)

	// Follower's currentCheckpointID == leader's checkpointId == 1.
	require.Equal(t, uint64(1), followerStore.GetCurrentCheckpointID())

	// --- Leader sends a Raft snapshot: checkpointId=1 at index 100 ---
	snapshotData, err := (&raftcmdpb.MemorySnapshot{CheckpointId: 1}).MarshalVT()
	require.NoError(t, err)

	err = followerMachine.InstallSnapshot(ctx, raftpb.Snapshot{
		Data:     snapshotData,
		Metadata: raftpb.SnapshotMetadata{Index: 100},
	})
	require.NoError(t, err)
	// fsm.lastCheckpointID == 1 == followerStore.GetCurrentCheckpointID():
	// the old code would skip the fetch here, leaving the follower with stale data.

	// --- Synchronize: must fetch leader's checkpoint regardless of ID collision ---
	appliedIndex, err := followerMachine.SynchronizeWithLeader(ctx, &copyDirFetcher{srcDir: leaderCheckpointPath}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), appliedIndex)

	// Follower store must now reflect the leader's checkpoint.
	_, err = query.GetLedgerByName(ctx, followerStore, "leader-ledger")
	require.NoError(t, err, "follower must have leader-ledger after sync")

	_, err = query.GetLedgerByName(ctx, followerStore, "follower-ledger")
	require.ErrorIs(t, err, domain.ErrNotFound, "stale follower-ledger must be gone after sync")
}
