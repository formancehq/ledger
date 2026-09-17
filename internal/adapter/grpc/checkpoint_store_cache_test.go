package grpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

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

	var (
		cache    checkpointStoreCache
		opens    atomic.Int64
		open     = realOpener(t, impl, &opens)
		wg       sync.WaitGroup
		releases = make(chan func(), 8)
	)

	for range cap(releases) {
		wg.Go(func() {
			main, readIdx, release, err := cache.acquire(t.Context(), gateCheckpointID, open)
			require.NoError(t, err)
			require.NotNil(t, main)
			require.NotNil(t, readIdx)
			releases <- release
		})
	}

	wg.Wait()
	close(releases)

	for release := range releases {
		release()
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

	_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, open)
	require.NoError(t, err)
	release()

	_, _, release, err = cache.acquire(t.Context(), gateCheckpointID, open)
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

	_, _, _, err := cache.acquire(t.Context(), gateCheckpointID, func() (*dal.Store, *readstore.Store, error) {
		return nil, nil, openFails
	})
	require.ErrorIs(t, err, openFails)
	require.Empty(t, cache.entries, "a failed open must leave nothing behind to serve")

	_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, succeed)
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

	_, _, _, err := cache.acquire(t.Context(), gateCheckpointID, func() (*dal.Store, *readstore.Store, error) {
		panic("pebble exploded")
	})
	require.ErrorContains(t, err, "panic opening checkpoint stores")
	require.Empty(t, cache.entries)

	done := make(chan struct{})

	go func() {
		defer close(done)

		_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, succeed)
		require.NoError(t, err)
		release()
	}()

	select {
	case <-done:
	case <-t.Context().Done():
		t.Fatal("a reader after a panicking open must not block")
	}
}

// A reader waiting on someone else's open must still honor its own deadline:
// sharing an open must not let one slow reader hold the rest past theirs.
func TestCheckpointStoreCacheJoinerHonorsCancellation(t *testing.T) {
	t.Parallel()

	impl := newCheckpointGateFixture(t)

	var (
		cache    checkpointStoreCache
		opens    atomic.Int64
		succeed  = realOpener(t, impl, &opens)
		opening  = make(chan struct{})
		unblock  = make(chan struct{})
		released = make(chan func(), 1)
	)

	var wg sync.WaitGroup

	wg.Go(func() {
		_, _, release, err := cache.acquire(t.Context(), gateCheckpointID, func() (*dal.Store, *readstore.Store, error) {
			close(opening)
			<-unblock

			return succeed()
		})
		require.NoError(t, err)
		released <- release
	})

	<-opening

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, release, err := cache.acquire(ctx, gateCheckpointID, succeed)
	require.ErrorIs(t, err, context.Canceled, "a cancelled joiner must not wait for the open")
	require.Nil(t, release)

	close(unblock)
	wg.Wait()
	(<-released)()

	require.Equal(t, int64(1), opens.Load(), "the cancelled joiner must not have started its own open")
	require.Empty(t, cache.entries, "the cancelled joiner's ref must have been dropped")
}
