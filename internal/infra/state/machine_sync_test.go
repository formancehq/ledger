package state

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// copyDirFetcher is a test SnapshotFetcher that copies a local directory into
// the target directory provided by the machine (simulating a real peer fetch).
type copyDirFetcher struct {
	srcDir string
}

func (f *copyDirFetcher) FetchSnapshot(_ context.Context, targetDir string, _ *SyncProgress, _ uint64) (uint64, error) {
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

	leaderBatch := leaderStore.OpenWriteSession()
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

	followerBatch := followerStore.OpenWriteSession()
	require.NoError(t, SetAppliedIndex(followerBatch, 90))
	require.NoError(t, followerBatch.Commit())

	// --- Leader sends a Raft snapshot at index 100 (no FSM data) ---
	followerRecovery := NewRecovery(followerMachine, followerStore)
	followerSync := NewSynchronizer(followerMachine, followerRecovery, dal.NewIncomingRestoreFactory(followerStore))
	err = followerSync.InstallSnapshot(ctx, raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 100},
	})
	require.NoError(t, err)

	// --- Synchronize: fetch leader's checkpoint, read lastAppliedIndex from Pebble ---
	appliedIndex, err := followerSync.SynchronizeWithLeader(ctx, &copyDirFetcher{srcDir: leaderCheckpointPath}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), appliedIndex)

	// Follower store must now reflect the leader's checkpoint.
	_, err = query.GetLedgerByName(ctx, followerStore, "leader-ledger")
	require.NoError(t, err, "follower must have leader-ledger after sync")

	_, err = query.GetLedgerByName(ctx, followerStore, "follower-ledger")
	require.ErrorIs(t, err, domain.ErrNotFound, "stale follower-ledger must be gone after sync")
}

// TestSynchronizeWithLeaderDrainsBackgroundChannels asserts that
// SynchronizeWithLeader empties the FSM's background-request channels before
// installing the leader's checkpoint. Pre-sync messages reference chapter IDs,
// sequence ranges and checkpoint paths from the follower's pre-sync state; if
// they survived the sync, the Archiver could write an empty SST over the
// leader's correct cold-storage archive (see #447 PR body).
func TestSynchronizeWithLeaderDrainsBackgroundChannels(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	meter := noop.NewMeterProvider().Meter("test")
	logger := logging.FromContext(logging.TestingContext())

	leaderStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = leaderStore.Close() })

	leaderBatch := leaderStore.OpenWriteSession()
	require.NoError(t, SetAppliedIndex(leaderBatch, 50))
	require.NoError(t, leaderBatch.Commit())

	_, err = leaderStore.CreateSnapshot()
	require.NoError(t, err)

	leaderCheckpointPath, err := leaderStore.GetCheckpointPath(1)
	require.NoError(t, err)

	followerMachine, followerStore, _ := newTestMachine(t)

	// Stuff every background channel with stale pre-sync work — what would
	// happen if the follower had applied a CloseChapter / ArchiveChapter /
	// chapter purge / cluster-config-change locally just before being told
	// to sync.
	require.True(t, followerMachine.sealRequestCh.TrySend(SealRequest{ChapterID: 99, CloseSequence: 999, CheckpointPath: "/tmp/stale"}, "stale-seal"))
	require.True(t, followerMachine.archiveRequestCh.TrySend(ArchiveRequest{ChapterID: 99, StartSequence: 1, CloseSequence: 999}, "stale-archive"))
	followerMachine.coldCompactionCh <- struct{}{}
	followerMachine.bloomRebuildCh <- "stale reason"

	followerRecovery := NewRecovery(followerMachine, followerStore)
	followerSync := NewSynchronizer(followerMachine, followerRecovery, dal.NewIncomingRestoreFactory(followerStore))
	require.NoError(t, followerSync.InstallSnapshot(ctx, raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 50}}))

	_, err = followerSync.SynchronizeWithLeader(ctx, &copyDirFetcher{srcDir: leaderCheckpointPath}, nil)
	require.NoError(t, err)

	// All channels must now be empty. Non-blocking receive should hit default.
	require.Len(t, followerMachine.sealRequestCh.Receive(), 0, "sealRequestCh leaked stale entry")
	require.Len(t, followerMachine.archiveRequestCh.Receive(), 0, "archiveRequestCh leaked stale entry")
	require.Len(t, followerMachine.coldCompactionCh, 0, "coldCompactionCh leaked stale signal")
	require.Len(t, followerMachine.bloomRebuildCh, 0, "bloomRebuildCh leaked stale reason")
}
