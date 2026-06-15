package dal

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestRestoreCheckpoint_RollsBackWhenReopenFails seeds a store, takes a
// checkpoint, then forces the post-rename reopen to fail by replacing
// the checkpoint directory with a broken file just before the restore.
// The restore must rollback: the original live data must still be
// readable and the store must remain usable.
//
// This is the regression for #189 — the previous implementation would
// have wiped live/ before discovering the reopen failure, leaving the
// node with no live store.
func TestRestoreCheckpoint_RollsBackWhenReopenFails(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Seed pre-restore data.
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("pre-restore"), []byte("survives-rollback")))
	require.NoError(t, batch.Commit())

	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)

	// Add more post-checkpoint data so a successful restore would lose it.
	postBatch := s.OpenWriteSession()
	require.NoError(t, postBatch.SetBytes([]byte("post-checkpoint"), []byte("would-be-lost")))
	require.NoError(t, postBatch.Commit())

	// Force the restore reopen to fail. The simplest way to make
	// pebble.Open(live/) blow up is to delete the checkpoint dir's
	// MANIFEST after capturing its inode list — but the hard-link is
	// the path, so the cleanest injection is to corrupt one of the
	// hard-linked Pebble files. Replace the OPTIONS-* file in the
	// checkpoint with garbage; Pebble refuses to open with an
	// unparseable OPTIONS file.
	checkpointDir := filepath.Join(s.DataDir(), checkpointsDir, strconv.FormatUint(checkpointID, 10))

	entries, err := os.ReadDir(checkpointDir)
	require.NoError(t, err)

	var corrupted bool

	for _, e := range entries {
		if filepath.Ext(e.Name()) == "" && len(e.Name()) > 8 && e.Name()[:8] == "OPTIONS-" {
			require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, e.Name()), []byte("garbage\n"), 0o644))

			corrupted = true

			break
		}
	}

	require.True(t, corrupted, "test setup: did not find an OPTIONS-* file to corrupt")

	// Attempt the restore — it must fail.
	restoreErr := s.RestoreCheckpoint(checkpointID)
	require.Error(t, restoreErr, "restore must fail after we corrupted the checkpoint OPTIONS file")

	// Now the regression check: the store must still be usable, and the
	// pre-restore data must still be there. The previous implementation
	// would have left s.db == nil and live/ wiped.
	val, closer, err := s.Get([]byte("pre-restore"))
	require.NoError(t, err, "pre-restore data must survive a failed restore")
	require.Equal(t, []byte("survives-rollback"), val)
	require.NoError(t, closer.Close())

	val, closer, err = s.Get([]byte("post-checkpoint"))
	require.NoError(t, err, "post-checkpoint data must survive a failed restore (rollback preserves live)")
	require.Equal(t, []byte("would-be-lost"), val)
	require.NoError(t, closer.Close())

	// live.discard/ must be cleaned up by the rollback.
	_, err = os.Stat(filepath.Join(s.DataDir(), liveDiscardDir))
	require.True(t, os.IsNotExist(err), "rollback must remove live.discard after reverting")

	// live.staging/ must also be cleaned up — the staged build was
	// abandoned and must not survive into the next call.
	_, err = os.Stat(filepath.Join(s.DataDir(), liveStagingDir))
	require.True(t, os.IsNotExist(err), "rollback must remove live.staging after reverting")
}

// TestReconcileLiveAfterRestore_CrashedBeforePublish is the regression
// flemzord asked for on PR #297. It simulates the exact crash window
// the old design conflated with success: the new database has been
// built and (in principle) all post-open work could have happened, but
// the atomic publish rename (staging -> live) never ran. Boot MUST
// drop staging and revert to live.discard, NOT mistake `live.staging`
// for proof of success and discard the rollback target.
func TestReconcileLiveAfterRestore_CrashedBeforePublish(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	// Boot once and write the pre-restore data that must survive the
	// aborted restore.
	s, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("pre-restore"), []byte("must-survive")))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.Close())

	// Simulate a mid-restore crash that landed after step 3 (rename-aside)
	// and after step 4-6 (build under staging), but before step 7 (publish
	// staging -> live). On disk: live/ is missing, live.staging/ exists
	// (a partially-built tree), live.discard/ holds the original.
	liveDirectory := filepath.Join(dataDir, liveDir)
	stagingDirectory := filepath.Join(dataDir, liveStagingDir)
	discardDirectory := filepath.Join(dataDir, liveDiscardDir)

	// Move the real live aside as the discard target.
	require.NoError(t, os.Rename(liveDirectory, discardDirectory))

	// Drop a staging tree alongside it — pebble doesn't need to be able
	// to open it; the reconciler removes it sight unseen.
	require.NoError(t, os.MkdirAll(stagingDirectory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stagingDirectory, "OPTIONS-000003"), []byte("garbage that would never open"), 0o644))

	// Boot. Reconciliation must drop staging AND revert discard -> live.
	s2, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err, "boot must reconcile aborted RestoreCheckpoint (staging present) and reopen original live")

	defer func() { _ = s2.Close() }()

	// The reverted live must contain the pre-restore data — the staging
	// tree must NOT have been mistaken for the committed restore.
	val, closer, err := s2.Get([]byte("pre-restore"))
	require.NoError(t, err, "pre-restore data must survive an aborted RestoreCheckpoint")
	require.Equal(t, []byte("must-survive"), val,
		"reconciler must NOT publish live.staging as live — the absence of live/ proves the restore never committed")
	require.NoError(t, closer.Close())

	// Staging must be gone.
	_, err = os.Stat(stagingDirectory)
	require.True(t, os.IsNotExist(err), "reconciliation must drop live.staging from an aborted restore")

	// Discard must be gone (it was consumed by the revert).
	_, err = os.Stat(discardDirectory)
	require.True(t, os.IsNotExist(err), "reconciliation must consume live.discard during revert")
}

// TestReconcileLiveAfterRestore_RevertsWhenLiveMissing simulates a
// process crash between the rename-aside and the checkpoint hard-link:
// live.discard/ is present, live/ is absent. NewStore must finish the
// rollback and reopen the original live.
func TestReconcileLiveAfterRestore_RevertsWhenLiveMissing(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	// Boot once and seed data so a real live directory exists on disk.
	s, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("survivor"), []byte("yes")))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.Close())

	// Simulate a crash mid-restore: rename live/ aside, leave nothing
	// in place. This is the state RestoreCheckpoint would leave behind
	// if it died between the rename and the hard-link.
	liveDirectory := filepath.Join(dataDir, liveDir)
	discardDirectory := filepath.Join(dataDir, liveDiscardDir)

	require.NoError(t, os.Rename(liveDirectory, discardDirectory))

	// Boot again. Reconciliation must revert and open the original DB.
	s2, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err, "boot must reconcile aborted RestoreCheckpoint and reopen")

	defer func() { _ = s2.Close() }()

	// The reverted live must contain the pre-restore data.
	val, closer, err := s2.Get([]byte("survivor"))
	require.NoError(t, err)
	require.Equal(t, []byte("yes"), val)
	require.NoError(t, closer.Close())

	// And live.discard/ must be gone.
	_, err = os.Stat(discardDirectory)
	require.True(t, os.IsNotExist(err), "reconciliation must consume live.discard")
}

// TestReconcileLiveAfterRestore_CleansUpWhenBothExist covers the
// post-success-pre-cleanup crash window: RestoreCheckpoint completed
// the swap but the process died before removing live.discard/. NewStore
// must keep the new live/ and drop the discard.
func TestReconcileLiveAfterRestore_CleansUpWhenBothExist(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("post-restore"), []byte("kept")))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.Close())

	// Drop a stale live.discard/ alongside the real live/. This is what
	// a crashed cleanup would leave on disk.
	discardDirectory := filepath.Join(dataDir, liveDiscardDir)
	require.NoError(t, os.MkdirAll(discardDirectory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(discardDirectory, "stale-file"), []byte("ignore"), 0o644))

	// Boot. Reconciliation must drop the discard without touching the live.
	s2, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = s2.Close() }()

	val, closer, err := s2.Get([]byte("post-restore"))
	require.NoError(t, err)
	require.Equal(t, []byte("kept"), val)
	require.NoError(t, closer.Close())

	_, err = os.Stat(discardDirectory)
	require.True(t, os.IsNotExist(err), "reconciliation must drop the stale live.discard")
}
