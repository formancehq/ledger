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

// TestCreateReadIndexCheckpointIsIdempotentOverStaleDir reproduces a
// replay-after-crash: a stale checkpoint directory (a previous attempt that died
// before writing the marker) already exists. createReadIndexCheckpoint must
// clear it and recreate cleanly rather than fail with pebble's ErrExist — this
// is what makes the crash-safe cursor ordering in processLogs correct.
func TestCreateReadIndexCheckpointIsIdempotentOverStaleDir(t *testing.T) {
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

	// The stale file must be gone (directory was recreated from scratch).
	_, err := os.Stat(filepath.Join(dir, "stale.sst"))
	require.True(t, os.IsNotExist(err), "stale file must be cleared on recreate")

	ro, err := readstore.OpenReadOnly(dir, logging.NopZap())
	require.NoError(t, err)
	require.NoError(t, ro.Close())
}
