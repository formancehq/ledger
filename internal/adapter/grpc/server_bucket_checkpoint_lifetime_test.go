package grpc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestOpenCheckpointStoresPinsFilesUntilRelease exercises the production
// acquisition boundary. Deletion may commit after both Pebble databases open,
// but their SST directories must remain available for later cache misses.
func TestOpenCheckpointStoresPinsFilesUntilRelease(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	logger := logging.NopZap()
	store, err := dal.NewStore(dataDir, logger, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	mainBatch := store.OpenWriteSession()
	require.NoError(t, mainBatch.SetBytes([]byte("main-key"), []byte("main-value")))
	require.NoError(t, mainBatch.Commit())

	const checkpointID = uint64(73)
	checkpointBatch := store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(checkpointBatch, &raftcmdpb.QueryCheckpointState{CheckpointId: checkpointID}))
	require.NoError(t, checkpointBatch.Commit())
	_, err = store.CreateQueryCheckpoint(checkpointID)
	require.NoError(t, err)

	liveReadStore, err := readstore.New(filepath.Join(dataDir, "live-read-index"), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, liveReadStore.Close()) })
	readBatch := liveReadStore.NewBatch()
	require.NoError(t, readBatch.SetBytes([]byte("read-key"), []byte("read-value")))
	require.NoError(t, readBatch.Commit())
	readIndexDir := store.QueryCheckpointReadIndexDir(checkpointID)
	require.NoError(t, liveReadStore.CreateCheckpoint(readIndexDir))
	require.NoError(t, readstore.MarkCheckpointReady(readIndexDir))

	server := &BucketServiceServerImpl{logger: logger, store: store}
	mainCheckpoint, readCheckpoint, release, err := server.openCheckpointStores(t.Context(), checkpointID)
	require.NoError(t, err)
	require.NoError(t, store.DeleteQueryCheckpointFiles(checkpointID))

	mainValue, mainCloser, err := mainCheckpoint.Get([]byte("main-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("main-value"), mainValue)
	require.NoError(t, mainCloser.Close())
	readValue, readCloser, err := readCheckpoint.DB().Get([]byte("read-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("read-value"), readValue)
	require.NoError(t, readCloser.Close())

	require.NoError(t, readCheckpoint.Close())
	require.NoError(t, mainCheckpoint.Close())
	release()
	_, err = os.Stat(filepath.Dir(store.QueryCheckpointMainDir(checkpointID)))
	require.True(t, os.IsNotExist(err))
}

func TestOpenCheckpointStoresReleasesLeaseOnReadinessFailure(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	logger := logging.NopZap()
	store, err := dal.NewStore(dataDir, logger, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	const checkpointID = uint64(74)
	checkpointBatch := store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(checkpointBatch, &raftcmdpb.QueryCheckpointState{CheckpointId: checkpointID}))
	require.NoError(t, checkpointBatch.Commit())
	checkpointDir := filepath.Dir(store.QueryCheckpointMainDir(checkpointID))
	require.NoError(t, os.MkdirAll(checkpointDir, 0o750))

	server := &BucketServiceServerImpl{logger: logger, store: store}
	_, _, _, err = server.openCheckpointStores(t.Context(), checkpointID)
	require.Error(t, err)

	require.NoError(t, store.DeleteQueryCheckpointFiles(checkpointID))
	_, err = os.Stat(checkpointDir)
	require.True(t, os.IsNotExist(err), "a failed open must not leak its filesystem lease")
}
