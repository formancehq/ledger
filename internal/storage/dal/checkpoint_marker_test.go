package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Readiness is keyed on the marker alone: an existing, even fully populated,
// directory without one is not ready.
func TestCheckpointDirReadyRequiresTheMarker(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("x"), 0o640))
	require.False(t, CheckpointDirReady(dir))

	require.NoError(t, MarkCheckpointReady(dir))
	require.True(t, CheckpointDirReady(dir))
	require.FileExists(t, filepath.Join(dir, checkpointReadyMarker))
}

func TestCheckpointDirReadyOnMissingDir(t *testing.T) {
	t.Parallel()

	require.False(t, CheckpointDirReady(filepath.Join(t.TempDir(), "absent")))
}

func TestMarkCheckpointReadyIsRepeatable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, MarkCheckpointReady(dir))
	require.NoError(t, MarkCheckpointReady(dir))
	require.True(t, CheckpointDirReady(dir))
}

func TestMarkCheckpointReadyOnMissingDir(t *testing.T) {
	t.Parallel()

	require.Error(t, MarkCheckpointReady(filepath.Join(t.TempDir(), "absent")))
}

func TestFsyncDir(t *testing.T) {
	t.Parallel()

	require.NoError(t, FsyncDir(t.TempDir()))
	require.Error(t, FsyncDir(filepath.Join(t.TempDir(), "absent")))
}
