package grpc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

const gateCheckpointID = uint64(3)

// newCheckpointGateFixture registers query checkpoint gateCheckpointID and
// materializes both of its halves, each marked ready.
func newCheckpointGateFixture(t *testing.T) *BucketServiceServerImpl {
	t.Helper()

	store, err := dal.NewStore(t.TempDir(), testLogger(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: gateCheckpointID}))
	require.NoError(t, batch.Commit())

	_, err = store.CreateQueryCheckpoint(gateCheckpointID)
	require.NoError(t, err)

	readIndex, err := readstore.New(t.TempDir(), testLogger(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = readIndex.Close() })

	readIndexPath := store.QueryCheckpointReadIndexDir(gateCheckpointID)
	require.NoError(t, readIndex.CreateCheckpoint(readIndexPath))
	require.NoError(t, dal.MarkCheckpointReady(readIndexPath))

	return &BucketServiceServerImpl{logger: testLogger(), store: store}
}

// forgetCheckpoint removes the registry row the way a committed delete does,
// before its files are unlinked.
func forgetCheckpoint(t *testing.T, store *dal.Store) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.DeleteQueryCheckpointFromBatch(batch, gateCheckpointID))
	require.NoError(t, batch.Commit())
}

func unmark(t *testing.T, dir string) {
	t.Helper()

	require.NoError(t, os.Remove(filepath.Join(dir, ".ready")))
}

// A read is served only when both halves are marked ready on this replica.
// Openability is not a completeness signal: pebble writes the MANIFEST that
// makes a directory openable before it copies the WAL files, so an unmarked
// main store opens cleanly while missing every memtable-resident write.
func TestOpenCheckpointStoresRequiresBothMarkers(t *testing.T) {
	t.Parallel()

	for name, unmarkHalf := range map[string]func(t *testing.T, impl *BucketServiceServerImpl){
		"main store unmarked": func(t *testing.T, impl *BucketServiceServerImpl) {
			unmark(t, impl.store.QueryCheckpointMainDir(gateCheckpointID))
		},
		"read index unmarked": func(t *testing.T, impl *BucketServiceServerImpl) {
			unmark(t, impl.store.QueryCheckpointReadIndexDir(gateCheckpointID))
		},
		"both unmarked": func(t *testing.T, impl *BucketServiceServerImpl) {
			unmark(t, impl.store.QueryCheckpointMainDir(gateCheckpointID))
			unmark(t, impl.store.QueryCheckpointReadIndexDir(gateCheckpointID))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			impl := newCheckpointGateFixture(t)
			unmarkHalf(t, impl)

			main, readIndex, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
			require.Nil(t, main, "an unmarked half must not be served")
			require.Nil(t, readIndex)

			notReady := &domain.ErrCheckpointNotReady{}
			require.ErrorAs(t, err, &notReady, "a registered checkpoint missing a marker is retryable")
			require.Equal(t, gateCheckpointID, notReady.CheckpointID)
			require.Equal(t, codes.Unavailable, status.Code(convertToGRPCError(err, testLogger())))
		})
	}
}

func TestOpenCheckpointStoresServesWhenBothMarked(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	main, readIndex, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
	require.NoError(t, err)
	require.NotNil(t, main)
	require.NotNil(t, readIndex)

	_ = main.Close()
	_ = readIndex.Close()
}

// Both markers present means the directory was complete when it was published,
// so a failing open is damage. It surfaces as a permanent error rather than the
// retryable ErrCheckpointNotReady, which would never heal.
func TestOpenCheckpointStoresSurfacesDamagedMainStore(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	manifests, err := filepath.Glob(filepath.Join(impl.store.QueryCheckpointMainDir(gateCheckpointID), "MANIFEST-*"))
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	require.NoError(t, os.Truncate(manifests[0], 0))

	main, readIndex, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
	require.Nil(t, main)
	require.Nil(t, readIndex)
	require.ErrorContains(t, err, "opening checkpoint main store")

	notReady := &domain.ErrCheckpointNotReady{}
	require.False(t, errors.As(err, &notReady), "damage must not be advertised as a materialization still in flight")
	var notFound *commonpb.NotFoundError
	require.False(t, errors.As(err, &notFound), "the checkpoint exists; only this replica's copy is damaged")
	require.Equal(t, codes.Unknown, status.Code(convertToGRPCError(err, testLogger())))
}

// A delete unlinks the directory under a read that already passed the marker
// check. The checkpoint is gone, so the read is permanently NotFound rather
// than the damage signal below.
func TestOpenCheckpointStoresReportsDeletedCheckpointAsNotFound(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	// RemoveAll unlinks children in readdir order, so the marker can outlive
	// the MANIFEST: the gate still sees both markers and the open still fails.
	forgetCheckpoint(t, impl.store)
	manifests, err := filepath.Glob(filepath.Join(impl.store.QueryCheckpointMainDir(gateCheckpointID), "MANIFEST-*"))
	require.NoError(t, err)
	require.NotEmpty(t, manifests)
	for _, manifest := range manifests {
		require.NoError(t, os.Remove(manifest))
	}

	main, readIndex, err := impl.openCheckpointStores(context.Background(), gateCheckpointID)
	require.Nil(t, main)
	require.Nil(t, readIndex)

	var notFound *commonpb.NotFoundError
	require.ErrorAs(t, err, &notFound)

	notReady := &domain.ErrCheckpointNotReady{}
	require.False(t, errors.As(err, &notReady), "a deleted checkpoint must not be advertised as retryable")
	require.Equal(t, codes.NotFound, status.Code(convertToGRPCError(err, testLogger())))
}
