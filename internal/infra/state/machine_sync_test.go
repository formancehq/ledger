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
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// copyDirFetcher is a test SnapshotFetcher that copies a local directory into
// the target directory provided by the machine (simulating a real peer fetch).
type copyDirFetcher struct {
	srcDir string
}

func (f *copyDirFetcher) FetchSnapshot(_ context.Context, targetDir string, _ *SyncProgress) (uint64, error) {
	if err := os.CopyFS(targetDir, os.DirFS(f.srcDir)); err != nil {
		return 0, err
	}

	return 0, nil
}

// TestSynchronizeWithLeader verifies that a follower correctly syncs from the
// leader by fetching a fresh checkpoint and reading lastAppliedIndex from Pebble.
func TestSynchronizeWithLeader(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	meter := noop.NewMeterProvider().Meter("test")
	logger := logging.FromContext(logging.TestingContext())

	// --- Leader store: "leader-ledger" at Raft index 100 ---
	leaderStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = leaderStore.Close() })

	registerLedger(t, leaderStore, "leader-ledger")

	leaderBatch := leaderStore.NewBatch()
	require.NoError(t, SetAppliedIndex(leaderBatch, 100))
	require.NoError(t, leaderBatch.Commit())

	// Create a checkpoint to simulate what FetchSnapshot would produce
	leaderCheckpointID, err := leaderStore.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), leaderCheckpointID)

	leaderCheckpointPath, err := leaderStore.GetCheckpointPath(1)
	require.NoError(t, err)

	// --- Follower machine: "follower-ledger" at Raft index 90 ---
	followerMachine, followerStore, _ := newTestMachine(t)

	registerLedger(t, followerStore, "follower-ledger")

	followerBatch := followerStore.NewBatch()
	require.NoError(t, SetAppliedIndex(followerBatch, 90))
	require.NoError(t, followerBatch.Commit())

	// --- Leader sends a Raft snapshot at index 100 (no FSM data) ---
	err = followerMachine.InstallSnapshot(ctx, raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 100},
	})
	require.NoError(t, err)

	// --- Synchronize: fetch leader's checkpoint, read lastAppliedIndex from Pebble ---
	appliedIndex, err := followerMachine.SynchronizeWithLeader(ctx, &copyDirFetcher{srcDir: leaderCheckpointPath}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), appliedIndex)

	// Follower store must now reflect the leader's checkpoint.
	_, err = query.GetLedgerByName(ctx, followerStore, "leader-ledger")
	require.NoError(t, err, "follower must have leader-ledger after sync")

	_, err = query.GetLedgerByName(ctx, followerStore, "follower-ledger")
	require.ErrorIs(t, err, domain.ErrNotFound, "stale follower-ledger must be gone after sync")
}
