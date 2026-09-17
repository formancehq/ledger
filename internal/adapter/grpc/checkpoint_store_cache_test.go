package grpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// acquireResult carries one goroutine's acquire outcome back to the test
// goroutine, which is the only one that may assert on it.
type acquireResult struct {
	main    *dal.Store
	readIdx *readstore.Store
	release func()
	err     error
}

// realOpener opens the fixture checkpoint through the same helper the handler
// uses, counting how many times it runs. The opens are genuine, so a test that
// reaches a second one while the first is still open fails the way production
// did.
func realOpener(t *testing.T, impl *BucketServiceServerImpl, opens *atomic.Int64) openCheckpointFn {
	t.Helper()

	return func() (*dal.Store, *readstore.Store, error) {
		opens.Add(1)

		return openCheckpointDirs(
			impl.store.QueryCheckpointMainDir(gateCheckpointID),
			impl.store.QueryCheckpointReadIndexDir(gateCheckpointID),
			testLogger(),
		)
	}
}

func TestCheckpointStoreCacheSharesOneOpenAcrossReaders(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	const readers = 8

	var (
		cache   checkpointStoreCache
		opens   atomic.Int64
		open    = realOpener(t, impl, &opens)
		wg      sync.WaitGroup
		results = make([]acquireResult, readers)
	)

	// Results are collected rather than asserted in the goroutines: require's
	// FailNow is a runtime.Goexit, which testify does not support off the test
	// goroutine, and it would skip the releases below.
	for i := range results {
		wg.Go(func() {
			main, readIdx, release, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), open)
			results[i] = acquireResult{main: main, readIdx: readIdx, release: release, err: err}
		})
	}

	wg.Wait()

	for _, result := range results {
		require.NoError(t, result.err)
		require.NotNil(t, result.main)
		require.NotNil(t, result.readIdx)
		result.release()
	}

	require.Equal(t, int64(1), opens.Load(), "concurrent readers of one checkpoint must share a single open")
}

// The last release must actually close both databases, not just forget them:
// Pebble still holds the directory lock until Close returns, so a reopen is the
// assertion that the close happened.
func TestCheckpointStoreCacheReopensAfterLastRelease(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache checkpointStoreCache
		opens atomic.Int64
		open  = realOpener(t, impl, &opens)
	)

	_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), open)
	require.NoError(t, err)
	release()

	_, _, release, err = cache.acquire(t.Context(), gateCheckpointID, testLogger(), open)
	require.NoError(t, err, "the previous open must have been closed")
	release()

	require.Equal(t, int64(2), opens.Load())
	require.Empty(t, cache.entries, "a cache at rest holds no handles")
}

func TestCheckpointStoreCacheDoesNotCacheAFailedOpen(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache     checkpointStoreCache
		opens     atomic.Int64
		succeed   = realOpener(t, impl, &opens)
		openFails = errors.New("open failed")
	)

	_, _, _, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), func() (*dal.Store, *readstore.Store, error) {
		return nil, nil, openFails
	})
	require.ErrorIs(t, err, openFails)
	require.Empty(t, cache.entries, "a failed open must leave nothing behind to serve")

	_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), succeed)
	require.NoError(t, err, "a later reader must be able to retry the open")
	release()
}

// A panicking open must not strand the checkpoint. The entry is installed
// before the open runs, so a panic that skipped closing done would leave every
// later reader of that id waiting on it for the life of the process.
func TestCheckpointStoreCacheSurvivesAPanickingOpen(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache   checkpointStoreCache
		opens   atomic.Int64
		succeed = realOpener(t, impl, &opens)
	)

	_, _, _, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), func() (*dal.Store, *readstore.Store, error) {
		panic("pebble exploded")
	})
	require.ErrorContains(t, err, "panic opening checkpoint stores")
	require.Empty(t, cache.entries)

	var (
		wg     sync.WaitGroup
		result acquireResult
	)

	wg.Go(func() {
		result.main, result.readIdx, result.release, result.err = cache.acquire(t.Context(), gateCheckpointID, testLogger(), succeed)
	})

	wg.Wait() // a reader after a panicking open must not block

	require.NoError(t, result.err)
	result.release()
}

// A reader waiting on someone else's open must still honor its own deadline:
// sharing an open must not let one slow reader hold the rest past theirs.
func TestCheckpointStoreCacheJoinerHonorsCancellation(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache   checkpointStoreCache
		opens   atomic.Int64
		succeed = realOpener(t, impl, &opens)
		opening = make(chan struct{})
		unblock = make(chan struct{})
	)

	var (
		wg     sync.WaitGroup
		opener acquireResult
	)

	wg.Go(func() {
		opener.main, opener.readIdx, opener.release, opener.err = cache.acquire(t.Context(), gateCheckpointID, testLogger(), func() (*dal.Store, *readstore.Store, error) {
			close(opening)
			<-unblock

			return succeed()
		})
	})

	<-opening

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, release, err := cache.acquire(ctx, gateCheckpointID, testLogger(), succeed)
	require.ErrorIs(t, err, context.Canceled, "a cancelled joiner must not wait for the open")
	require.Nil(t, release)

	close(unblock)
	wg.Wait()
	require.NoError(t, opener.err)
	opener.release()

	require.Equal(t, int64(1), opens.Load(), "the cancelled joiner must not have started its own open")
	require.Empty(t, cache.entries, "the cancelled joiner's ref must have been dropped")
}

// pebble.DB.Close panics with "element has outstanding references" when a file
// cache reference on it is still held, so the two closes must not share a
// failure path: a panic out of the read index's close would otherwise skip the
// main store's and leave its directory locked against every later reader.
func TestCheckpointStoreCacheClosesMainStoreWhenReadIndexCloseFails(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	mainPath := impl.store.QueryCheckpointMainDir(gateCheckpointID)
	readIndexPath := impl.store.QueryCheckpointReadIndexDir(gateCheckpointID)

	// The panic needs a reference into an SST, so the read index's checkpoint is
	// rebuilt here from flushed data; the fixture's fits in a memtable.
	require.NoError(t, os.RemoveAll(readIndexPath))
	rebuildReadIndexCheckpointWithSSTs(t, readIndexPath)

	var cache checkpointStoreCache

	_, readIdx, release, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), func() (*dal.Store, *readstore.Store, error) {
		return openCheckpointDirs(mainPath, readIndexPath, testLogger())
	})
	require.NoError(t, err)

	iter, err := readIdx.DB().NewIter(nil)
	require.NoError(t, err)
	require.True(t, iter.First(), "the iterator must hold an SST-backed reference")

	release()

	reopened, err := dal.OpenReadOnly(mainPath, testLogger())
	require.NoError(t, err, "the main store must close even when the read index's close panics")
	require.NoError(t, reopened.Close())
	require.NoError(t, iter.Close())
}

// rebuildReadIndexCheckpointWithSSTs materializes a read-index checkpoint at
// path whose data sits in an SST rather than a memtable.
func rebuildReadIndexCheckpointWithSSTs(t *testing.T, path string) {
	t.Helper()

	const keys = 20_000

	live, err := readstore.New(t.TempDir(), testLogger(), readstore.DefaultConfig())
	require.NoError(t, err)

	batch := live.NewBatch()
	for i := range keys {
		require.NoError(t, batch.SetBytes(fmt.Appendf(nil, "bench/%08d", i), make([]byte, 256)))
	}
	require.NoError(t, batch.Commit())
	require.NoError(t, live.DB().Flush())

	require.NoError(t, live.CreateCheckpoint(path))
	require.NoError(t, dal.MarkCheckpointReady(path))
	require.NoError(t, live.Close())

	ssts, err := filepath.Glob(filepath.Join(path, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, ssts, "the checkpoint must contain an SST for the reference to exist")
}

// The stores are shared, so a defensive second cleanup must not drop a hold the
// caller does not have and close them under the checkpoint's other readers.
func TestCheckpointStoreCacheReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache checkpointStoreCache
		opens atomic.Int64
		open  = realOpener(t, impl, &opens)
	)

	_, _, releaseFirst, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), open)
	require.NoError(t, err)

	main, _, releaseSecond, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), open)
	require.NoError(t, err)

	releaseFirst()
	releaseFirst()

	value, closer, err := main.Get([]byte("any-key"))
	require.ErrorIs(t, err, pebble.ErrNotFound, "the shared stores must still be open for the remaining reader")
	require.Nil(t, value)
	require.Nil(t, closer)

	releaseSecond()
	require.Empty(t, cache.entries)
}
