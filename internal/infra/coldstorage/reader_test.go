package coldstorage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// ComputeSHA256OrPanic is a test helper that returns the SHA-256 of b. It
// panics on any read error (impossible from a bytes.Reader) to keep call
// sites short — only used in tests that already constructed b above.
func ComputeSHA256OrPanic(b []byte) []byte {
	c, err := ComputeSHA256(bytes.NewReader(b))
	if err != nil {
		panic(err)
	}

	return c
}

// inMemoryColdStorage is a test-only in-memory ColdStorage for ColdReader tests.
type inMemoryColdStorage struct {
	mu        sync.Mutex
	data      map[string][]byte
	checksums map[string][]byte
}

func newInMemoryColdStorage() *inMemoryColdStorage {
	return &inMemoryColdStorage{
		data:      make(map[string][]byte),
		checksums: make(map[string][]byte),
	}
}

func (m *inMemoryColdStorage) Archive(_ context.Context, bucketID string, chapterID uint64, data io.Reader, sha256 []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%d", bucketID, chapterID)
	m.data[key] = buf
	m.checksums[key] = append([]byte(nil), sha256...)

	return nil
}

func (m *inMemoryColdStorage) Exists(_ context.Context, bucketID string, chapterID uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, chapterID)
	_, hasData := m.data[key]
	_, hasChecksum := m.checksums[key]

	return hasData && hasChecksum, nil
}

func (m *inMemoryColdStorage) ExpectedChecksum(_ context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	c, ok := m.checksums[fmt.Sprintf("%s/%d", bucketID, chapterID)]
	if !ok {
		return nil, ErrChecksumNotFound
	}

	return append([]byte(nil), c...), nil
}

func (m *inMemoryColdStorage) Checksum(_ context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, chapterID)

	buf, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("archive %s not found", key)
	}

	return ComputeSHA256(bytes.NewReader(buf))
}

func (m *inMemoryColdStorage) Fetch(_ context.Context, bucketID string, chapterID uint64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s/%d", bucketID, chapterID)

	buf, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("archive %s not found", key)
	}

	return io.NopCloser(bytes.NewReader(buf)), nil
}

// buildTestSST creates a minimal SST file with the given key-value pairs (must be sorted).
func buildTestSST(t *testing.T, kvs [][2][]byte) []byte {
	t.Helper()

	var buf bytes.Buffer

	writer := sstable.NewWriter(newBufWritable(&buf), sstable.WriterOptions{
		Compression:  sstable.SnappyCompression,
		FilterPolicy: bloom.FilterPolicy(10),
	})

	for _, kv := range kvs {
		require.NoError(t, writer.Set(kv[0], kv[1]))
	}

	require.NoError(t, writer.Close())

	return buf.Bytes()
}

// bufWritable adapts a bytes.Buffer to objstorage.Writable.
type bufWritable struct {
	buf *bytes.Buffer
}

func newBufWritable(buf *bytes.Buffer) *bufWritable {
	return &bufWritable{buf: buf}
}

func (w *bufWritable) Write(p []byte) error {
	_, err := w.buf.Write(p)

	return err
}

func (w *bufWritable) Finish() error { return nil }
func (w *bufWritable) Abort()        {}

func TestColdReaderCacheMiss(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	// Store a test SST in cold storage
	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	// First call: cache miss → download + ingest
	pebbleReader, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, pebbleReader)
	defer func() { require.NoError(t, pebbleReader.Close()) }()

	// Verify the data is readable
	val, closer, err := pebbleReader.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	_ = closer.Close()

	val, closer, err = pebbleReader.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), val)
	_ = closer.Close()
}

func TestColdReaderCacheHit(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	// First call
	r1, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)

	// Second call: should return the same cached DB
	r2, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)

	// Both leases should reference the same cached DB.
	require.Same(t, r1.(*readHandle).cached, r2.(*readHandle).cached)
	require.NoError(t, r1.Close())
	require.NoError(t, r2.Close())
}

func TestColdReaderEviction(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	// Create SSTs for chapters 1, 2, 3
	for i := uint64(1); i <= 3; i++ {
		sstData := buildTestSST(t, [][2][]byte{
			{fmt.Appendf(nil, "key-%d", i), fmt.Appendf(nil, "value-%d", i)},
		})
		require.NoError(t, cs.Archive(ctx, "bucket", i, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))
	}

	// maxCached=2 → opening a 3rd chapter should evict the oldest
	reader := NewColdReader(cs, "bucket", cacheDir, 2, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	r1, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, r1.Close())
	r2, err := reader.GetReader(ctx, 2)
	require.NoError(t, err)
	require.NoError(t, r2.Close())

	// This should evict chapter 1
	r3, err := reader.GetReader(ctx, 3)
	require.NoError(t, err)
	defer func() { require.NoError(t, r3.Close()) }()

	val, closer, err := r3.Get([]byte("key-3"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-3"), val)
	_ = closer.Close()

	// Verify chapter 1 was evicted from cache (directory cleaned up)
	_, err = os.Stat(cacheDir + "/chapter-1")
	require.True(t, os.IsNotExist(err), "chapter-1 cache directory should have been removed")

	// Chapter 2 should still be cached
	r2, err = reader.GetReader(ctx, 2)
	require.NoError(t, err)
	defer func() { require.NoError(t, r2.Close()) }()

	val, closer, err = r2.Get([]byte("key-2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-2"), val)
	_ = closer.Close()
}

func TestColdReaderCapacityEvictionKeepsAcquiredReaderAlive(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	for chapterID := uint64(1); chapterID <= 2; chapterID++ {
		sstData := buildTestSST(t, [][2][]byte{
			{fmt.Appendf(nil, "key-%d", chapterID), fmt.Appendf(nil, "value-%d", chapterID)},
		})
		require.NoError(t, cs.Archive(ctx, "bucket", chapterID, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))
	}

	reader := NewColdReader(cs, "bucket", cacheDir, 1, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	acquired := make(chan struct{})
	evicted := make(chan struct{})
	readResult := make(chan error, 1)

	go func() {
		chapter, err := reader.GetReader(ctx, 1)
		if err != nil {
			readResult <- err

			return
		}
		defer func() { _ = chapter.Close() }()

		close(acquired)
		<-evicted

		value, closer, err := chapter.Get([]byte("key-1"))
		if err != nil {
			readResult <- err

			return
		}
		defer func() { _ = closer.Close() }()

		if !bytes.Equal(value, []byte("value-1")) {
			readResult <- fmt.Errorf("unexpected value: %q", value)

			return
		}

		readResult <- nil
	}()

	<-acquired
	chapter2, err := reader.GetReader(ctx, 2)
	require.NoError(t, err)
	require.NoError(t, chapter2.Close())
	close(evicted)

	require.NoError(t, <-readResult, "an acquired reader must remain usable after cache eviction")
}

func TestColdReaderTTLEvictionWaitsForLastReader(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()
	sstData := buildTestSST(t, [][2][]byte{{[]byte("key"), []byte("value")}})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 4, time.Hour, logger)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	first, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	second, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)

	reader.mu.Lock()
	reader.cache[1].lastAccess = time.Now().Add(-2 * time.Hour)
	reader.mu.Unlock()
	reader.sweepExpired()

	_, err = os.Stat(filepath.Join(cacheDir, "chapter-1"))
	require.NoError(t, err, "an evicted chapter stays resident while readers hold leases")
	require.NoError(t, first.Close())

	_, err = os.Stat(filepath.Join(cacheDir, "chapter-1"))
	require.NoError(t, err, "the first of multiple releases must not close the database")
	value, closer, err := second.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	require.NoError(t, closer.Close())
	require.NoError(t, second.Close())

	_, err = os.Stat(filepath.Join(cacheDir, "chapter-1"))
	require.True(t, os.IsNotExist(err), "the last release must close and remove an evicted chapter")
}

func TestColdReaderEvictedChapterCanBeReacquired(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()
	sstData := buildTestSST(t, [][2][]byte{{[]byte("key"), []byte("value")}})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 1, 0, logger)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	first, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)

	reader.mu.Lock()
	reader.evictByID(1)
	reader.mu.Unlock()

	second, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.Same(t, first.(*readHandle).cached, second.(*readHandle).cached)

	reader.mu.Lock()
	reader.evictByID(1)
	reader.evictByID(1)
	reader.mu.Unlock()

	require.NoError(t, first.Close())
	value, closer, err := second.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	require.NoError(t, closer.Close())
	require.NoError(t, second.Close())

	_, err = os.Stat(filepath.Join(cacheDir, "chapter-1"))
	require.True(t, os.IsNotExist(err), "repeated eviction must still release the database exactly once")
}

func TestColdReaderCloseWaitsForActiveReader(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()
	sstData := buildTestSST(t, [][2][]byte{{[]byte("key"), []byte("value")}})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 1, 0, logger)
	handle, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)

	closeResult := make(chan error, 1)
	go func() { closeResult <- reader.Close() }()
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()

		return reader.closed
	}, time.Second, time.Millisecond, "shutdown must first refuse new leases")

	select {
	case err := <-closeResult:
		require.Failf(t, "Close returned with an active reader", "error: %v", err)
	default:
	}

	value, closer, err := handle.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
	require.NoError(t, closer.Close())
	require.NoError(t, handle.Close())
	require.NoError(t, <-closeResult)
	require.NoError(t, reader.Close(), "Close must be idempotent")

	_, err = reader.GetReader(ctx, 1)
	require.EqualError(t, err, "cold reader is closed")
}

func TestColdReaderLRUTouchOrder(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	for i := uint64(1); i <= 3; i++ {
		sstData := buildTestSST(t, [][2][]byte{
			{fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, "v%d", i)},
		})
		require.NoError(t, cs.Archive(ctx, "bucket", i, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))
	}

	reader := NewColdReader(cs, "bucket", cacheDir, 2, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	// Load chapters 1 and 2
	r1, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, r1.Close())
	r2, err := reader.GetReader(ctx, 2)
	require.NoError(t, err)
	require.NoError(t, r2.Close())

	// Touch chapter 1 (moves it to end of LRU)
	r1, err = reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, r1.Close())

	// Load chapter 3 → should evict chapter 2 (oldest untouched), not chapter 1
	r3, err := reader.GetReader(ctx, 3)
	require.NoError(t, err)
	require.NoError(t, r3.Close())

	// Chapter 1 should still be cached
	_, err = os.Stat(cacheDir + "/chapter-1")
	require.NoError(t, err, "chapter-1 should still be cached")

	// Chapter 2 should be evicted
	_, err = os.Stat(cacheDir + "/chapter-2")
	require.True(t, os.IsNotExist(err), "chapter-2 should have been evicted")
}

func TestColdReaderClose(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 4, 0, logger)

	handle, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	// Close should clean up
	require.NoError(t, reader.Close())

	// Cache directory for the chapter should be removed
	_, err = os.Stat(cacheDir + "/chapter-1")
	require.True(t, os.IsNotExist(err), "cache directory should be cleaned after Close")
}

func TestColdReaderFetchNotFound(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	reader := NewColdReader(cs, "bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	// Chapter 999 doesn't exist in cold storage
	_, err := reader.GetReader(ctx, 999)
	require.Error(t, err)
}

func TestColdReaderPebbleQueries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	// Build SST with multiple keys for iterator testing
	sstData := buildTestSST(t, [][2][]byte{
		{[]byte{0x01, 0x00, 0x01}, []byte("log-1")},
		{[]byte{0x01, 0x00, 0x02}, []byte("log-2")},
		{[]byte{0x01, 0x00, 0x03}, []byte("log-3")},
		{[]byte{0x02, 0x00, 0x01}, []byte("audit-1")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	reader := NewColdReader(cs, "bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	pebbleReader, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	defer func() { require.NoError(t, pebbleReader.Close()) }()

	// Test iterator over prefix 0x01
	iter, err := pebbleReader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0x01},
		UpperBound: []byte{0x02},
	})
	require.NoError(t, err)

	var count int

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Close())
	require.Equal(t, 3, count, "should have 3 entries with prefix 0x01")

	// Test Get on exact key
	val, closer, err := pebbleReader.Get([]byte{0x02, 0x00, 0x01})
	require.NoError(t, err)
	require.Equal(t, []byte("audit-1"), val)
	_ = closer.Close()

	// Test Get on non-existent key
	_, _, err = pebbleReader.Get([]byte{0x03, 0x00, 0x01})
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestColdReaderTTLEviction(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	// TTL of 100ms, sweep runs every 50ms
	reader := NewColdReader(cs, "bucket", cacheDir, 4, 100*time.Millisecond, logger)
	t.Cleanup(func() { _ = reader.Close() })

	handle, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	// Cache directory should exist
	_, err = os.Stat(cacheDir + "/chapter-1")
	require.NoError(t, err, "chapter-1 should be cached")

	// Wait for TTL + sweep interval to expire
	require.Eventually(t, func() bool {
		_, err := os.Stat(cacheDir + "/chapter-1")

		return os.IsNotExist(err)
	}, 1*time.Second, 25*time.Millisecond, "chapter-1 should be evicted after TTL")
}

func TestColdReaderTTLRefreshedOnAccess(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	cs := newInMemoryColdStorage()
	cacheDir := t.TempDir()

	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
	})
	require.NoError(t, cs.Archive(ctx, "bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	// TTL of 200ms
	reader := NewColdReader(cs, "bucket", cacheDir, 4, 200*time.Millisecond, logger)
	t.Cleanup(func() { _ = reader.Close() })

	handle, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	cachePath := cacheDir + "/chapter-1"

	// Keep refreshing the entry beyond its original TTL. Never polls the state
	// before each refresh, so an eviction or refresh error fails immediately.
	require.Never(t, func() bool {
		if _, statErr := os.Stat(cachePath); statErr != nil {
			return true
		}

		handle, getErr := reader.GetReader(ctx, 1)
		if getErr == nil {
			getErr = handle.Close()
		}

		return getErr != nil
	}, 400*time.Millisecond, 80*time.Millisecond, "chapter-1 should remain cached while accesses refresh its TTL")

	// Should still be cached (each access refreshes the TTL)
	_, err = os.Stat(cachePath)
	require.NoError(t, err, "chapter-1 should still be cached after repeated access")
}
