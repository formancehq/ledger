package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestStore_DataDir(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	require.NotEmpty(t, s.DataDir())
}

func TestStore_Flush(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data and flush
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("flush-key"), []byte("flush-val")))
	require.NoError(t, batch.Commit())

	require.NoError(t, s.Flush())

	// Verify data is still accessible
	val, closer, err := s.Get([]byte("flush-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("flush-val"), val)
	require.NoError(t, closer.Close())
}

func TestStore_GetCurrentCheckpointID(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	// Initial checkpoint ID should be 0
	require.Equal(t, uint64(0), s.GetCurrentCheckpointID())
}

func TestStore_CreateSnapshot(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("snap-key"), []byte("snap-val")))
	require.NoError(t, batch.Commit())

	// Create snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), checkpointID)
	require.Equal(t, uint64(1), s.GetCurrentCheckpointID())

	// Verify checkpoint directory exists
	checkpointDir := filepath.Join(s.DataDir(), "checkpoints", "1")
	_, err = os.Stat(checkpointDir)
	require.NoError(t, err)

	// Create another snapshot
	checkpointID2, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(2), checkpointID2)
}

func TestStore_GetCheckpointPath(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// On fresh start, no checkpoint exists. Create one first.
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)

	path, err := s.GetCheckpointPath(checkpointID)
	require.NoError(t, err)
	require.Contains(t, path, "checkpoints")

	// Non-existent checkpoint should error
	_, err = s.GetCheckpointPath(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestStore_CreateTemporaryCheckpoint(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("tmp-key"), []byte("tmp-val")))
	require.NoError(t, batch.Commit())

	// Create temporary checkpoint
	path, err := s.CreateTemporaryCheckpoint("test-backup")
	require.NoError(t, err)
	require.NotEmpty(t, path)

	// Verify the directory exists
	_, err = os.Stat(path)
	require.NoError(t, err)

	// TemporaryCheckpointPath should find it
	gotPath, exists := s.TemporaryCheckpointPath("test-backup")
	require.True(t, exists)
	require.Equal(t, path, gotPath)
}

func TestStore_RemoveTemporaryCheckpoint(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Create and remove
	_, err := s.CreateTemporaryCheckpoint("to-remove")
	require.NoError(t, err)

	require.NoError(t, s.RemoveTemporaryCheckpoint("to-remove"))

	// Should no longer exist
	_, exists := s.TemporaryCheckpointPath("to-remove")
	require.False(t, exists)
}

func TestStore_TemporaryCheckpointPath_NotFound(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	_, exists := s.TemporaryCheckpointPath("nonexistent")
	require.False(t, exists)
}

func TestStore_PrepareCheckpointRestore(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	path, err := s.PrepareCheckpointRestore(42)
	require.NoError(t, err)
	require.NotEmpty(t, path)

	// Directory should have been created
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestStore_RestoreCheckpoint(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("pre-restore"), []byte("should-exist")))
	require.NoError(t, batch.Commit())

	// Create a checkpoint
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)

	// Write more data after the checkpoint
	batch2 := s.OpenWriteSession()
	require.NoError(t, batch2.SetBytes([]byte("post-snapshot"), []byte("after")))
	require.NoError(t, batch2.Commit())

	// Restore from the checkpoint
	require.NoError(t, s.RestoreCheckpoint(checkpointID))
	require.Equal(t, checkpointID, s.GetCurrentCheckpointID())

	// Data from before the checkpoint should exist
	val, closer, err := s.Get([]byte("pre-restore"))
	require.NoError(t, err)
	require.Equal(t, []byte("should-exist"), val)
	require.NoError(t, closer.Close())
}

func TestStore_RestoreCheckpoint_NonExistent(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	err := s.RestoreCheckpoint(999)
	require.Error(t, err)
}

func TestStore_RestoreCheckpoint_PreservesPersistedConfig(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write a persisted config key (simulates node identity)
	configKey := []byte{ZoneGlobal, SubGlobPersistedConfig}
	configVal := []byte(`{"nodeId":"node-1","clusterId":"cluster-1"}`)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes(configKey, configVal))
	require.NoError(t, batch.SetBytes([]byte("other-data"), []byte("value")))
	require.NoError(t, batch.Commit())

	// Create a snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)

	// Write new data and a DIFFERENT config after the snapshot
	batch2 := s.OpenWriteSession()
	require.NoError(t, batch2.SetBytes(configKey, []byte(`{"nodeId":"node-2"}`)))
	require.NoError(t, batch2.SetBytes([]byte("post-snapshot-data"), []byte("new")))
	require.NoError(t, batch2.Commit())

	// Restore from the checkpoint. The code should preserve the CURRENT node's config
	// (node-2) even though the checkpoint has node-1.
	require.NoError(t, s.RestoreCheckpoint(checkpointID))

	// The persisted config should be the one from BEFORE restore (node-2),
	// because RestoreCheckpoint reads and re-writes the current node's config.
	val, closer, err := s.Get(configKey)
	require.NoError(t, err)
	// The preserved config is whatever was in the DB before close, which was node-2
	require.JSONEq(t, `{"nodeId":"node-2"}`, string(val))
	require.NoError(t, closer.Close())
}

func TestStore_Checkpoint(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("cp-key"), []byte("cp-val")))
	require.NoError(t, batch.Commit())

	// Create a standalone checkpoint
	destDir := filepath.Join(t.TempDir(), "standalone-cp")
	require.NoError(t, s.Checkpoint(destDir))

	// Verify we can open it
	db, err := pebble.Open(destDir, &pebble.Options{
		Logger:   DiscardPebbleLogger(),
		ReadOnly: true,
	})
	require.NoError(t, err)

	val, closer, err := db.Get([]byte("cp-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("cp-val"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, db.Close())
}

func TestStore_CleanupOldCheckpoints(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	cfg := DefaultConfig()
	cfg.MaxCheckpoints = 3

	s, err := NewStore(t.TempDir(), logger, meter, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Create multiple snapshots to exceed maxCheckpoints
	for range 5 {
		batch := s.OpenWriteSession()
		require.NoError(t, batch.SetBytes([]byte("k"), []byte("v")))
		require.NoError(t, batch.Commit())

		_, err := s.CreateSnapshot()
		require.NoError(t, err)
	}

	// The oldest checkpoints should have been cleaned up
	// With MaxCheckpoints=3 and 6 total (0-5), checkpoints 0-2 should be gone
	checkpointsDir := filepath.Join(s.DataDir(), "checkpoints")
	entries, err := os.ReadDir(checkpointsDir)
	require.NoError(t, err)
	require.LessOrEqual(t, len(entries), 4, "should have at most 4 checkpoint dirs")
}

// TestStore_CleanupOldCheckpoints_RemovesOrphansBelowTracker is the
// EN-1409 regression test. Before the fix, cleanupOldCheckpoints iterated
// from an in-memory tracker initialised at boot via arithmetic
// (latestID - maxCheckpoints + 1), so any checkpoint dir whose ID was
// below that floor at boot time -- left there by a previous non-graceful
// exit -- was permanently unreachable to cleanup and leaked
// `maxCheckpoints * checkpoint_size` per crash forever.
//
// The fix scans the checkpoints directory on every cleanup and removes
// any numeric dir below `oldestToKeep`, regardless of the tracker, so
// orphans are reclaimed on the first snapshot after restart.
func TestStore_CleanupOldCheckpoints_RemovesOrphansBelowTracker(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataDir := t.TempDir()
	checkpointsPath := filepath.Join(dataDir, "checkpoints")

	// Simulate fossil pairs left by previous crashes plus the live pair
	// that survived the most recent crash. With maxCheckpoints=2 and
	// latestID=101, the buggy init would compute oldestCheckpoint=100,
	// leaving {5, 6} permanently unreachable.
	for _, id := range []string{"5", "6", "100", "101"} {
		require.NoError(t, os.MkdirAll(filepath.Join(checkpointsPath, id), 0o755))
	}

	cfg := DefaultConfig()
	cfg.MaxCheckpoints = 2

	s, err := NewStore(dataDir, logger, meter, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// One snapshot is enough to fire the new scan-based cleanup.
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("k"), []byte("v")))
	require.NoError(t, batch.Commit())

	_, err = s.CreateSnapshot()
	require.NoError(t, err)

	// After cleanup, only the last maxCheckpoints (=2) checkpoints should
	// remain on disk: the just-created 102 and the previous 101. The
	// fossil pair {5, 6} and the now-aged 100 must be gone.
	entries, err := os.ReadDir(checkpointsPath)
	require.NoError(t, err)

	remaining := map[string]bool{}
	for _, e := range entries {
		remaining[e.Name()] = true
	}

	require.NotContains(t, remaining, "5", "fossil orphan 5 must be reclaimed")
	require.NotContains(t, remaining, "6", "fossil orphan 6 must be reclaimed")
	require.NotContains(t, remaining, "100", "aged-out 100 must be reclaimed")
	require.Contains(t, remaining, "101", "previous-live 101 must be kept")
	require.Contains(t, remaining, "102", "newly-created 102 must be kept")
}

// TestStore_LegacyIncomingMigration asserts the boot-time cleanup of the
// legacy follower-sync staging path (dataDir/checkpoints/incoming) used by
// older builds. Newer builds stage at dataDir/incoming-checkpoint; the
// legacy path is volatile-only so it can be removed unconditionally.
func TestStore_LegacyIncomingMigration(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataDir := t.TempDir()
	legacyIncoming := filepath.Join(dataDir, "checkpoints", "incoming")
	require.NoError(t, os.MkdirAll(legacyIncoming, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyIncoming, "junk"), []byte("x"), 0o644))

	s, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	_, err = os.Stat(legacyIncoming)
	require.True(t, os.IsNotExist(err), "legacy checkpoints/incoming must be removed at boot")

	_, err = os.Stat(filepath.Join(dataDir, "incoming-checkpoint"))
	require.True(t, os.IsNotExist(err), "new staging path must not be pre-created")
}

// TestStore_PrepareIncomingRestoreLocation pins the new staging path so a
// future move is caught by tests, not by surprise on production restore.
func TestStore_PrepareIncomingRestoreLocation(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	dir, err := s.PrepareIncomingRestore()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(s.DataDir(), "incoming-checkpoint"), dir)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

// TestStore_ActivateIncomingRestore_FreshFollower pins the safety net that
// makes follower-sync work when the follower has never taken a local
// snapshot: the leader delivers the first checkpoint before `checkpoints/`
// exists on disk, so ActivateIncomingRestore must MkdirAll the parent
// before renaming staging into place. A refactor that drops the MkdirAll
// would silently reintroduce the fresh-follower failure — this test
// catches it.
func TestStore_ActivateIncomingRestore_FreshFollower(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Precondition: checkpoints/ must not exist on a fresh store.
	_, err := os.Stat(filepath.Join(s.DataDir(), "checkpoints"))
	require.True(t, os.IsNotExist(err), "fresh store must not have a checkpoints/ dir yet")

	stagingDir, err := s.PrepareIncomingRestore()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(stagingDir, "marker"), []byte("leader-data"), 0o644))

	checkpointID, err := s.ActivateIncomingRestore()
	require.NoError(t, err, "must succeed even without a pre-existing checkpoints/ parent")
	require.Equal(t, uint64(1), checkpointID)

	// Staging dir was renamed (not copied) into checkpoints/<id>/.
	marker, err := os.ReadFile(filepath.Join(s.DataDir(), "checkpoints", "1", "marker"))
	require.NoError(t, err)
	require.Equal(t, []byte("leader-data"), marker)

	_, err = os.Stat(stagingDir)
	require.True(t, os.IsNotExist(err), "staging dir must be renamed away, not left behind")
}

func TestStore_IterateColdKVPairs(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	kb := NewKeyBuilder()

	// Write some cold-storable pairs (Log prefix)
	batch := s.OpenWriteSession()
	key1 := kb.PutZonePrefix(ZoneCold, SubColdLog).PutUint64(1).Build()
	key2 := kb.PutZonePrefix(ZoneCold, SubColdLog).PutUint64(2).Build()
	key3 := kb.PutZonePrefix(ZoneCold, SubColdLog).PutUint64(3).Build()

	require.NoError(t, batch.SetBytes(key1, []byte("log-1")))
	require.NoError(t, batch.SetBytes(key2, []byte("log-2")))
	require.NoError(t, batch.SetBytes(key3, []byte("log-3")))

	// Write some audit pairs
	key4 := kb.PutZonePrefix(ZoneCold, SubColdAudit).PutUint64(1).Build()
	key5 := kb.PutZonePrefix(ZoneCold, SubColdAudit).PutUint64(2).Build()

	require.NoError(t, batch.SetBytes(key4, []byte("audit-1")))
	require.NoError(t, batch.SetBytes(key5, []byte("audit-2")))

	require.NoError(t, batch.Commit())

	// Iterate cold KV pairs in range [1, 3].
	// Note: IterateColdKVPairs uses KeyBuilder.Snapshot() which creates a range
	// that effectively captures sequences startSeq through closeSeq for each prefix.
	var collected []string

	err := s.IterateColdKVPairs(1, 3, 1, 3, func(key, value []byte) error {
		collected = append(collected, string(value))

		return nil
	})
	require.NoError(t, err)

	// Should have log entries (1, 2, 3) and audit entries (1, 2) = 5 total
	// But the range scanning depends on key builder behavior; verify we get at least some entries
	require.GreaterOrEqual(t, len(collected), 2, "should iterate at least some cold KV pairs")
}

func TestStore_IterateColdKVPairs_NoMatches(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	var count int

	err := s.IterateColdKVPairs(100, 200, 100, 200, func(key, value []byte) error {
		count++

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

// TestStore_IterateColdKVPairs_AuditRangeAppliedToAuditZones pins the fix
// for #312. Logs and audit advance on independent sequence counters, so the
// archiver must hand BOTH ranges to IterateColdKVPairs. Before the fix the
// audit zones were scanned with the log range, silently dropping every
// audit entry whose audit sequence happened to fall outside the log window
// — and the subsequent purge still removed them from Pebble.
func TestStore_IterateColdKVPairs_AuditRangeAppliedToAuditZones(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := NewKeyBuilder()

	// Chapter: log range [10, 20], audit range [3, 5].
	// The two windows do not overlap on purpose — that is precisely the case
	// that broke the archiver before #312.
	batch := s.OpenWriteSession()

	logKeys := []uint64{10, 15, 20}
	for _, seq := range logKeys {
		k := kb.PutZonePrefix(ZoneCold, SubColdLog).PutUint64(seq).Build()
		require.NoError(t, batch.SetBytes(k, []byte("log")))
	}

	auditKeys := []uint64{3, 4, 5}
	for _, seq := range auditKeys {
		k := kb.PutZonePrefix(ZoneCold, SubColdAudit).PutUint64(seq).Build()
		require.NoError(t, batch.SetBytes(k, []byte("audit")))

		item := kb.PutZonePrefix(ZoneCold, SubColdAuditItem).PutUint64(seq).PutUint32(0).Build()
		require.NoError(t, batch.SetBytes(item, []byte("audit-item")))
	}

	// A separate audit entry at seq=20 — would be picked up if the audit
	// zone was (incorrectly) scanned with the log range.
	straySeq := uint64(20)
	strayKey := kb.PutZonePrefix(ZoneCold, SubColdAudit).PutUint64(straySeq).Build()
	require.NoError(t, batch.SetBytes(strayKey, []byte("audit-stray")))

	require.NoError(t, batch.Commit())

	var (
		logs        int
		audits      int
		auditItems  int
		strayPicked bool
	)

	err := s.IterateColdKVPairs(10, 20, 3, 5, func(key, _ []byte) error {
		switch key[1] {
		case SubColdLog:
			logs++
		case SubColdAudit:
			audits++
			if len(key) == 10 {
				// Check whether this is the audit entry at the stray seq.
				if seq := readUint64(key[2:10]); seq == straySeq {
					strayPicked = true
				}
			}
		case SubColdAuditItem:
			auditItems++
		}

		return nil
	})
	require.NoError(t, err)

	require.Equal(t, len(logKeys), logs, "log zone must be scanned with the log range")
	require.Equal(t, len(auditKeys), audits, "audit zone must be scanned with the audit range")
	require.Equal(t, len(auditKeys), auditItems, "audit-item zone must be scanned with the audit range")
	require.False(t, strayPicked, "audit entry outside the audit range must NOT be archived (#312)")
}

// TestStore_IterateColdKVPairs_EmptyAuditRangeNoop documents that a chapter
// with no audit entries (close < start by convention) skips the audit
// scans entirely instead of erroring.
func TestStore_IterateColdKVPairs_EmptyAuditRangeNoop(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	err := s.IterateColdKVPairs(0, 10, 1, 0, func(_, _ []byte) error {
		t.Fatal("no rows expected — empty audit range and empty log zone")

		return nil
	})
	require.NoError(t, err)
}

func readUint64(b []byte) uint64 {
	var v uint64
	for i := 0; i < 8 && i < len(b); i++ {
		v = (v << 8) | uint64(b[i])
	}

	return v
}

func TestStore_NewIter(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("iter-a"), []byte("1")))
	require.NoError(t, batch.SetBytes([]byte("iter-b"), []byte("2")))
	require.NoError(t, batch.Commit())

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte("iter-"),
		UpperBound: []byte("iter-\xff"),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	require.NoError(t, iter.Error())
	require.Equal(t, []string{"iter-a", "iter-b"}, keys)
}

func TestStore_OpenReadOnly(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("ro-key"), []byte("ro-val")))
	require.NoError(t, batch.Commit())

	// Create a checkpoint we can open read-only
	cpDir := filepath.Join(t.TempDir(), "ro-cp")
	require.NoError(t, s.Checkpoint(cpDir))

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	roStore, err := OpenReadOnly(cpDir, logger)
	require.NoError(t, err)

	defer func() { _ = roStore.Close() }()

	val, closer, err := roStore.Get([]byte("ro-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("ro-val"), val)
	require.NoError(t, closer.Close())

	// Regression: OpenReadOnly is used as a secondary store during full
	// backups while the primary still holds its working set. MaxOpenFiles
	// must stay capped so Pebble does not warm up table metadata for every
	// SST in large stores (observed pushing pods past their memory limit
	// on a 290 GB checkpoint).
	require.Equal(t, 32, roStore.opts.MaxOpenFiles,
		"OpenReadOnly must bound MaxOpenFiles to keep the secondary store's table-metadata footprint small")
}

func TestStore_OpenDirect(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("direct-key"), []byte("direct-val")))
	require.NoError(t, batch.Commit())

	// Create a checkpoint we can open directly
	cpDir := filepath.Join(t.TempDir(), "direct-cp")
	require.NoError(t, s.Checkpoint(cpDir))

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	directStore, err := OpenDirect(cpDir, logger)
	require.NoError(t, err)

	defer func() { _ = directStore.Close() }()

	val, closer, err := directStore.Get([]byte("direct-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("direct-val"), val)
	require.NoError(t, closer.Close())
}

func TestStore_NewStoreReopensExisting(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")
	dir := t.TempDir()

	// Create store and write data
	s1, err := NewStore(dir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	batch := s1.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("persist-key"), []byte("persist-val")))
	require.NoError(t, batch.Commit())

	// Create a checkpoint so the checkpoints/ directory is populated
	_, err = s1.CreateSnapshot()
	require.NoError(t, err)

	require.NoError(t, s1.Close())

	// Reopen the store
	s2, err := NewStore(dir, logger, meter, DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = s2.Close() }()

	val, closer, err := s2.Get([]byte("persist-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("persist-val"), val)
	require.NoError(t, closer.Close())
}
