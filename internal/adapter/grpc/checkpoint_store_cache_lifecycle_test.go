package grpc

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

type panicOnListFS struct{ vfs.FS }

func (panicOnListFS) List(string) ([]string, error) {
	panic("filesystem listing after lock acquisition")
}

func TestPebbleOpenPanicReleasesDirectoryLock(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	path := impl.store.QueryCheckpointMainDir(gateCheckpointID)
	require.PanicsWithValue(t, "filesystem listing after lock acquisition", func() {
		_, _ = pebble.Open(path, &pebble.Options{ReadOnly: true, FS: panicOnListFS{vfs.Default}})
	})
	store, err := dal.OpenReadOnly(path, testLogger())
	require.NoError(t, err, "Pebble unwinds its directory lock before propagating an open-time panic")
	require.NoError(t, store.Close())
}

func TestCheckpointStoreCacheJoinedWaiterHonorsCancellation(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	var cache checkpointStoreCache
	var opens atomic.Int64
	open := realOpener(t, impl, &opens)
	opening, unblock := make(chan struct{}), make(chan struct{})
	first, joined := make(chan acquireResult, 1), make(chan acquireResult, 1)
	ctx, cancel := context.WithCancel(t.Context())
	var joinedResult *acquireResult
	joinerStarted := false
	// Drain every worker and release every acquired handle even when the
	// cancellation assertion fails and the joiner succeeds after unblocking.
	receive := func(ch <-chan acquireResult, name string) *acquireResult {
		t.Helper()
		select {
		case result := <-ch:
			return &result
		case <-time.After(5 * time.Second):
			t.Errorf("%s did not finish during cleanup", name)

			return nil
		}
	}
	t.Cleanup(func() {
		cancel()
		close(unblock)
		firstResult := receive(first, "opener")
		if joinerStarted && joinedResult == nil {
			joinedResult = receive(joined, "joiner")
		}
		for _, result := range []*acquireResult{firstResult, joinedResult} {
			if result != nil && result.release != nil {
				result.release()
			}
		}
		if firstResult == nil || (joinerStarted && joinedResult == nil) {
			return // Worker timeout was reported; do not inspect active state.
		}
		require.NoError(t, firstResult.err)
		cache.evict(gateCheckpointID)
		cache.mu.Lock()
		remaining := len(cache.entries)
		cache.mu.Unlock()
		require.Zero(t, remaining)
	})
	go func() {
		main, idx, release, err := cache.acquire(t.Context(), gateCheckpointID, testLogger(), func() (*dal.Store, *readstore.Store, error) {
			close(opening)
			<-unblock

			return open()
		})
		first <- acquireResult{main, idx, release, err}
	}()
	select {
	case <-opening:
	case <-time.After(5 * time.Second):
		t.Fatal("opener did not reach the synchronization point")
	}
	joinerStarted = true
	go func() {
		main, idx, release, err := cache.acquire(ctx, gateCheckpointID, testLogger(), open)
		joined <- acquireResult{main, idx, release, err}
	}()
	// Unlike cancelling before acquire, this proves the joined select branch.
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()

		return cache.entries[gateCheckpointID].refs == 2
	}, 5*time.Second, time.Millisecond)
	cancel()
	select {
	case result := <-joined:
		joinedResult = &result
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.release)
	case <-time.After(5 * time.Second):
		t.Fatal("joined waiter stayed blocked after cancellation")
	}
	cache.mu.Lock()
	refs := cache.entries[gateCheckpointID].refs
	cache.mu.Unlock()
	require.Equal(t, 1, refs, "cancellation must drop only the joiner's reference")
}

func TestOpenCheckpointStoresDeletionWaitsForBothReaders(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	_, _, first, err := impl.openCheckpointStores(t.Context(), gateCheckpointID)
	require.NoError(t, err)
	defer first()
	main, index, second, err := impl.openCheckpointStores(t.Context(), gateCheckpointID)
	require.NoError(t, err)
	defer second()
	require.NoError(t, impl.store.DeleteQueryCheckpointFiles(gateCheckpointID))
	dir := filepath.Dir(impl.store.QueryCheckpointMainDir(gateCheckpointID))
	require.DirExists(t, dir)
	first()
	require.DirExists(t, dir, "one reader still holds the filesystem lease")
	handle, err := main.NewReadHandle()
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	iter, err := index.DB().NewIter(nil)
	require.NoError(t, err)
	require.NoError(t, iter.Close())
	release, acquired := impl.store.AcquireQueryCheckpoint(gateCheckpointID)
	if acquired {
		release()
	}
	require.False(t, acquired, "a reader arriving after deletion must be refused")
	second()
	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err), "the final release must remove the directory")
}

func TestOpenCheckpointStoresDeletionClosesIdlePair(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	mainPath := impl.store.QueryCheckpointMainDir(gateCheckpointID)
	_, _, release, err := impl.openCheckpointStores(t.Context(), gateCheckpointID)
	require.NoError(t, err)
	release()
	require.Contains(t, impl.checkpointStores.entries, gateCheckpointID)

	require.NoError(t, impl.store.DeleteQueryCheckpointFiles(gateCheckpointID))
	require.NotContains(t, impl.checkpointStores.entries, gateCheckpointID)
	require.NoDirExists(t, filepath.Dir(mainPath))
}

func TestCheckpointDeletionBeforeCacheInstallationClosesLeasedPair(t *testing.T) {
	t.Parallel()
	impl := newCheckpointGateFixture(t)
	id := gateCheckpointID
	mainPath := impl.store.QueryCheckpointMainDir(id)
	indexPath := impl.store.QueryCheckpointReadIndexDir(id)
	impl.store.RegisterQueryCheckpointDeleteHook(impl.checkpointStores.evict)

	// The reader has its filesystem lease, but is still between readiness
	// checks and cache installation when deletion commits.
	releaseLease, ok := impl.store.AcquireQueryCheckpoint(id)
	require.True(t, ok)
	require.NoError(t, impl.store.DeleteQueryCheckpointFiles(id))
	require.DirExists(t, filepath.Dir(mainPath), "the lease pins files")

	_, _, releasePair, err := impl.acquireCheckpointPair(t.Context(), id, mainPath, indexPath)
	require.NoError(t, err)
	releasePair()
	require.NotContains(t, impl.checkpointStores.entries, id, "the deleted pair must not become idle")
	releaseLease()
	require.NoDirExists(t, filepath.Dir(mainPath))
}
