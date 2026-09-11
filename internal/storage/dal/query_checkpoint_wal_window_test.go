package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestQueryCheckpointIsOpenableBeforeItIsComplete pins the window a query
// checkpoint read can land in.
//
// pebble.DB.Checkpoint writes the destination directory in this order
// (pebble/checkpoint.go): create the directory, copy OPTIONS, link the
// sstables, write the MANIFEST — which is what makes the directory openable —
// and only THEN copy the WAL files. WithFlushedWAL syncs the WAL, it does not
// flush the memtable, so every write not yet in an sstable lives in those WAL
// files.
//
// So between the MANIFEST write and the WAL copy the directory opens cleanly
// and serves a state missing every memtable-resident write. This test stages
// that state by removing the copied WALs and shows the open still succeeds,
// silently, with the data gone.
func TestQueryCheckpointIsOpenableBeforeItIsComplete(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Written and committed, but nothing forces an sstable, so the value is
	// memtable-resident and belongs to the WAL.
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("committed-key"), []byte("committed-value")))
	require.NoError(t, batch.Commit())

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	logger := logging.FromContext(logging.TestingContext())

	// A completed checkpoint vouches for itself, so readers have a signal that
	// does not depend on the open succeeding.
	require.True(t, CheckpointDirReady(dir), "a completed checkpoint must be marked ready")

	// Control: the complete checkpoint carries the write.
	complete, err := OpenReadOnly(dir, logger)
	require.NoError(t, err)

	val, closer, err := complete.Get([]byte("committed-key"))
	require.NoError(t, err, "a complete checkpoint must carry a committed write")
	require.Equal(t, []byte("committed-value"), val)
	require.NoError(t, closer.Close())
	require.NoError(t, complete.Close())

	// Stage the window: MANIFEST present, WALs not yet copied.
	wals, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	require.NotEmpty(t, wals, "the checkpoint must carry WAL files for this window to exist")

	for _, w := range wals {
		require.NoError(t, os.Remove(w))
	}

	// The open is the only gate the main store has, and it passes.
	partial, err := OpenReadOnly(dir, logger)
	require.NoError(t, err, "a checkpoint missing its WALs still opens — the open is not a completeness gate")

	defer func() { _ = partial.Close() }()

	_, _, err = partial.Get([]byte("committed-key"))
	require.Error(t, err, "the staged checkpoint serves a state missing a committed write")
}

// TestCreateQueryCheckpointRedundantCallIsNoOp pins that a second checkpoint for
// the same id succeeds against the already-materialized directory. The applier
// re-runs this while replaying the spool or the WAL after a restart, and pebble
// rejects an existing destination, so the redundant call has to be recognized
// and skipped — the same guard the read index half carries.
func TestCreateQueryCheckpointRedundantCallIsNoOp(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.True(t, CheckpointDirReady(dir))

	sentinel := filepath.Join(dir, "sentinel.marker")
	require.NoError(t, os.WriteFile(sentinel, []byte("x"), 0o640))

	again, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.Equal(t, dir, again)
	require.True(t, CheckpointDirReady(again))
	require.FileExists(t, sentinel, "a redundant call must not rebuild the directory")
}

// A temp directory left by a crash must not poison every later attempt for that
// id: pebble refuses an existing destination, so the stale one is cleared first.
func TestCreateQueryCheckpointClearsStaleTempDir(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	tmpDir := dir + ".tmp"
	require.NoError(t, os.Rename(dir, tmpDir))

	again, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.Equal(t, dir, again)
	require.True(t, CheckpointDirReady(again))
	require.NoDirExists(t, tmpDir)
}

// TestCreateQueryCheckpointRebuildsUnmarkedDir pins the crash case: a final
// directory carrying no readiness marker is a prior attempt that died before
// vouching for itself, so it is discarded and rebuilt rather than trusted or
// rejected as already existing.
func TestCreateQueryCheckpointRebuildsUnmarkedDir(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, checkpointReadyMarker)))
	require.False(t, CheckpointDirReady(dir))

	rebuilt, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.Equal(t, dir, rebuilt)
	require.True(t, CheckpointDirReady(rebuilt))
}

// TestCreateQueryCheckpointLeavesNoTempDir pins that the temp directory the
// checkpoint is built under does not survive a successful materialization —
// otherwise every checkpoint would leave a full second copy of the store on
// disk.
func TestCreateQueryCheckpointLeavesNoTempDir(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	_, err = os.Stat(dir + ".tmp")
	require.True(t, os.IsNotExist(err), "temp checkpoint directory outlived the rename")
}
