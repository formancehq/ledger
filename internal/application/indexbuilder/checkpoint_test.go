package indexbuilder

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// newCheckpointTestBuilder builds a minimal Builder wired with a real dal.Store
// (for checkpoint directory paths) and readstore.Store (source of the read-index
// checkpoint), enough to exercise createReadIndexCheckpoint end to end.
func newCheckpointTestBuilder(t *testing.T) *Builder {
	t.Helper()

	dataDir := t.TempDir()
	meter := noop.NewMeterProvider().Meter("test")

	pebbleStore, err := dal.NewStore(dataDir, logging.NopZap(), meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pebbleStore.Close() })

	readStore, err := readstore.New(filepath.Join(dataDir, "readindex-root"), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = readStore.Close() })

	return &Builder{
		pebbleStore: pebbleStore,
		readStore:   readStore,
		logger:      logging.NopZap(),
	}
}

// TestCreateReadIndexCheckpointWritesReadyMarker verifies the happy path: the
// checkpoint directory is created and the .ready marker is written last, so the
// checkpoint reads as ready and is openable.
func TestCreateReadIndexCheckpointWritesReadyMarker(t *testing.T) {
	t.Parallel()

	b := newCheckpointTestBuilder(t)

	const cpID = uint64(7)
	require.NoError(t, b.createReadIndexCheckpoint(cpID))

	dir := b.pebbleStore.QueryCheckpointReadIndexDir(cpID)
	require.True(t, readstore.CheckpointDirReady(dir), "readiness marker must exist after creation")

	ro, err := readstore.OpenReadOnly(dir, logging.NopZap())
	require.NoError(t, err)
	require.NoError(t, ro.Close())
}

// TestCreateReadIndexCheckpointRebuildsUnmarkedDir reproduces a markerless
// directory left by a crash between rename and marker (or a pre-marker build):
// createReadIndexCheckpoint must discard it and rebuild atomically rather than
// trust or fail on it — a markerless directory is never adopted.
func TestCreateReadIndexCheckpointRebuildsUnmarkedDir(t *testing.T) {
	t.Parallel()

	b := newCheckpointTestBuilder(t)

	const cpID = uint64(11)
	dir := b.pebbleStore.QueryCheckpointReadIndexDir(cpID)

	// Simulate a stale, markerless directory left by a crashed prior attempt.
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stale.sst"), []byte("garbage"), 0o640))
	require.False(t, readstore.CheckpointDirReady(dir))

	// Recreation must succeed and produce a ready, openable checkpoint.
	require.NoError(t, b.createReadIndexCheckpoint(cpID))
	require.True(t, readstore.CheckpointDirReady(dir))

	// The stale file must be gone (directory was rebuilt from scratch).
	_, err := os.Stat(filepath.Join(dir, "stale.sst"))
	require.True(t, os.IsNotExist(err), "stale file must be cleared on rebuild")

	ro, err := readstore.OpenReadOnly(dir, logging.NopZap())
	require.NoError(t, err)
	require.NoError(t, ro.Close())
}

// TestCreateReadIndexCheckpointLeavesNoTempDir verifies the atomic
// materialization cleans up its temp directory: after a successful build only
// the final directory exists, never the sibling ".tmp".
func TestCreateReadIndexCheckpointLeavesNoTempDir(t *testing.T) {
	t.Parallel()

	b := newCheckpointTestBuilder(t)

	const cpID = uint64(13)
	require.NoError(t, b.createReadIndexCheckpoint(cpID))

	finalDir := b.pebbleStore.QueryCheckpointReadIndexDir(cpID)
	require.True(t, readstore.CheckpointDirReady(finalDir))

	_, err := os.Stat(finalDir + ".tmp")
	require.True(t, os.IsNotExist(err), "temp dir must not survive a successful materialization")
}

// TestCreateReadIndexCheckpointNoopWhenReady verifies a redundant call on an
// already-ready checkpoint is a cheap no-op (does not rebuild or error).
func TestCreateReadIndexCheckpointNoopWhenReady(t *testing.T) {
	t.Parallel()

	b := newCheckpointTestBuilder(t)

	const cpID = uint64(17)
	require.NoError(t, b.createReadIndexCheckpoint(cpID))

	dir := b.pebbleStore.QueryCheckpointReadIndexDir(cpID)
	// Drop a sentinel; a no-op second call must leave it untouched.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sentinel"), nil, 0o640))

	require.NoError(t, b.createReadIndexCheckpoint(cpID))

	_, err := os.Stat(filepath.Join(dir, "sentinel"))
	require.NoError(t, err, "ready checkpoint must not be rebuilt on a redundant call")
}
