package balancehistoryarchive

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCacheValidatesConfigurationAndScansExistingRuns(t *testing.T) {
	t.Parallel()

	_, err := newCache("", 1)
	require.ErrorContains(t, err, "cache directory is required")
	_, err = newCache(t.TempDir(), 0)
	require.ErrorContains(t, err, "max bytes must be positive")

	notDirectory := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notDirectory, []byte("x"), 0o600))
	_, err = newCache(notDirectory, 1)
	require.ErrorContains(t, err, "creating balance history archive cache")

	cacheDir := t.TempDir()
	blob, ref := encodeBlob(t, testRecords("startup-scan", 8))
	require.NoError(t, os.WriteFile(
		filepath.Join(cacheDir, ref.Hex()+cacheFileSuffix),
		blob,
		0o444,
	))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "ignored.tmp"), []byte("temporary"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(cacheDir, "short.run"), []byte("short"), 0o600))
	require.NoError(t, os.WriteFile(
		filepath.Join(cacheDir, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.run"),
		[]byte("invalid hex"),
		0o600,
	))
	require.NoError(t, os.Mkdir(filepath.Join(cacheDir, ref.Hex()+"00"+cacheFileSuffix), 0o755))

	cache, err := newCache(cacheDir, int64(len(blob))+1)
	require.NoError(t, err)
	require.Equal(t, CacheStats{Bytes: int64(len(blob)), Entries: 1}, cache.stats())

	evicted, err := newCache(cacheDir, 1)
	require.NoError(t, err)
	require.Equal(t, CacheStats{}, evicted.stats())
	require.NoFileExists(t, filepath.Join(cacheDir, ref.Hex()+cacheFileSuffix))
}

func TestCacheAcquireReportsLoadFailureAndCancellation(t *testing.T) {
	t.Parallel()

	cache, err := newCache(t.TempDir(), 1024)
	require.NoError(t, err)
	_, ref := encodeBlob(t, testRecords("load-error", 8))
	wantErr := errors.New("load failed")
	_, hit, err := cache.acquire(context.Background(), ref, func() (preparedFile, error) {
		return preparedFile{}, wantErr
	})
	require.False(t, hit)
	require.ErrorIs(t, err, wantErr)

	call := &cacheCall{done: make(chan struct{})}
	cache.inflight[ref.SHA256] = call
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, hit, err = cache.acquire(canceled, ref, func() (preparedFile, error) {
		require.FailNow(t, "load must not run while another load is in flight")

		return preparedFile{}, nil
	})
	require.False(t, hit)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCacheInternalGuards(t *testing.T) {
	t.Parallel()

	cache, err := newCache(t.TempDir(), 1024)
	require.NoError(t, err)
	_, ref := encodeBlob(t, testRecords("guards", 8))

	cache.mu.Lock()
	_, err = cache.publishLocked(preparedFile{}, ref, 0)
	require.ErrorContains(t, err, "prepared cache file path is required")
	_, err = cache.publishLocked(preparedFile{path: "prepared", size: int64(ref.Size) - 1}, ref, 0)
	require.ErrorContains(t, err, "prepared cache file size mismatch")
	cache.mu.Unlock()

	entry := &cacheEntry{ref: ref, leases: 1}
	cache.mu.Lock()
	err = cache.removeLocked(entry)
	cache.mu.Unlock()
	require.ErrorContains(t, err, "cannot evict a leased")

	err = cache.installIndex(&cacheEntry{ref: ref}, &recordIndex{})
	require.ErrorContains(t, err, "cannot index an evicted")

	indexed := &cacheEntry{ref: ref}
	indexed.element = cache.lru.PushBack(indexed)
	cache.entries[ref.SHA256] = indexed
	require.NoError(t, cache.installIndex(indexed, &recordIndex{offsets: []uint64{1, 2}}))
	require.NoError(t, cache.installIndex(indexed, &recordIndex{offsets: []uint64{3}}))
	require.Equal(t, int64(16), indexed.indexBytes)

	negative := &cacheEntry{ref: Ref{SHA256: [32]byte{1}}, path: filepath.Join(t.TempDir(), "absent"), size: 1}
	negative.element = cache.lru.PushBack(negative)
	cache.entries[negative.ref.SHA256] = negative
	cache.mu.Lock()
	cache.bytes = 0
	err = cache.removeLocked(negative)
	cache.mu.Unlock()
	require.ErrorContains(t, err, "negative balance history archive cache bytes")

	require.Error(t, fsyncDirectory(filepath.Join(t.TempDir(), "missing")))
}

func TestLeaseRejectsUseAfterCloseAndUnderflow(t *testing.T) {
	t.Parallel()

	closed := &Lease{done: true}
	_, err := closed.Open()
	require.ErrorContains(t, err, "lease is closed")
	_, err = closed.OpenIndexed()
	require.ErrorContains(t, err, "lease is closed")
	require.NoError(t, closed.Close())
	require.NoError(t, closed.invalidate())

	cache, err := newCache(t.TempDir(), 1024)
	require.NoError(t, err)
	entry := &cacheEntry{}
	lease := &Lease{cache: cache, entry: entry}
	err = lease.Close()
	require.ErrorContains(t, err, "lease underflow")

	entry = &cacheEntry{}
	lease = &Lease{cache: cache, entry: entry}
	err = lease.invalidate()
	require.ErrorContains(t, err, "lease underflow")
}
