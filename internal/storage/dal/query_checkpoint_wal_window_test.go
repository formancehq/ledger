package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestQueryCheckpointIncludesCommittedWrites checks RocksDB checkpoint's
// flush-backed copy: committed writes remain available even when no WAL files
// are carried by the checkpoint.
func TestQueryCheckpointIncludesCommittedWrites(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("committed-key"), []byte("committed-value")))
	require.NoError(t, batch.Commit())

	dir, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.True(t, CheckpointDirReady(dir))

	// kv.Checkpoint forces a flush, so the copied SST carries this write.
	// Remove any copied WALs to make that property explicit.
	wals, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	for _, wal := range wals {
		require.NoError(t, os.Remove(wal))
	}

	reader, err := OpenReadOnly(dir, logging.FromContext(logging.TestingContext()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	val, closer, err := reader.Get([]byte("committed-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("committed-value"), val)
	require.NoError(t, closer.Close())
}

// TestCreateQueryCheckpointRedundantCallIsNoOp pins that a second checkpoint for
// the same id keeps the materialized directory instead of rebuilding it: RocksDB
// rejects an existing destination, so the call has to recognize and skip it —
// the same guard the read index half carries.
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
// id: RocksDB refuses an existing destination, so the stale one is cleared first.
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

	stale := filepath.Join(dir, "stale.sst")
	require.NoError(t, os.WriteFile(stale, []byte("x"), 0o640))

	rebuilt, err := s.CreateQueryCheckpoint(1)
	require.NoError(t, err)
	require.Equal(t, dir, rebuilt)
	require.True(t, CheckpointDirReady(rebuilt))
	require.NoFileExists(t, stale, "the unmarked directory must be discarded, not reused")
}

// TestCreateQueryCheckpointOnClosedStore pins that the failure path surfaces the
// store's own error rather than a partially built directory.
func TestCreateQueryCheckpointOnClosedStore(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	require.NoError(t, s.Close())

	_, err := s.CreateQueryCheckpoint(1)
	require.ErrorIs(t, err, ErrStoreClosed)
	require.NoDirExists(t, s.QueryCheckpointMainDir(1))
	require.NoDirExists(t, s.QueryCheckpointMainDir(1)+".tmp")
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
