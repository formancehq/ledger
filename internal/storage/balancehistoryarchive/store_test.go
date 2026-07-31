package balancehistoryarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

func TestArchiveIsContentAddressedAndIdempotent(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	store := newTestStore(t, cold, t.TempDir(), 1<<20)
	records := testRecords("idempotent", 128)

	first, err := store.Archive(context.Background(), NewSliceStream(records))
	require.NoError(t, err)
	second, err := store.Archive(context.Background(), NewSliceStream(records))
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 1, cold.archiveCount())

	keys := cold.keys()
	require.Len(t, keys, 1)
	require.Equal(t, fmt.Sprintf("cluster/balance-history/nodes/node-1/runs/%s#0", first.Hex()), keys[0])

	exists, err := store.Exists(context.Background(), first)
	require.NoError(t, err)
	require.True(t, exists)

	lease, err := store.Fetch(context.Background(), first)
	require.NoError(t, err)
	require.Equal(t, records, readLease(t, lease))
}

func TestFetchDistinguishesMissingAndCorruptObjects(t *testing.T) {
	t.Parallel()

	t.Run("missing", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		store := newTestStore(t, cold, t.TempDir(), 1<<20)
		_, ref := encodeBlob(t, testRecords("missing", 16))

		_, err := store.Fetch(context.Background(), ref)
		require.ErrorIs(t, err, ErrMissing)
		var missing *MissingError
		require.ErrorAs(t, err, &missing)
		require.Equal(t, ref, missing.Ref)

		exists, err := store.Exists(context.Background(), ref)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("downloaded bytes", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		blob, ref := encodeBlob(t, testRecords("corrupt-data", 32))
		cold.seed("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID, blob, ref.SHA256[:])
		cold.corruptData("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID)
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		_, err := store.Fetch(context.Background(), ref)
		require.ErrorIs(t, err, ErrCorrupt)
		_, err = store.Exists(context.Background(), ref)
		require.ErrorIs(t, err, ErrCorrupt)
	})

	t.Run("expected checksum", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		blob, ref := encodeBlob(t, testRecords("corrupt-checksum", 32))
		bucket := "cluster/balance-history/nodes/node-1/runs/" + ref.Hex()
		cold.seed(bucket, archiveChapterID, blob, bytes.Repeat([]byte{0xee}, sha256.Size))
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		_, err := store.Fetch(context.Background(), ref)
		require.ErrorIs(t, err, ErrCorrupt)
	})

	t.Run("embedded codec checksum", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		blob, ref := encodeBlob(t, testRecords("embedded", 32))
		blob[len(blob)-trailerSize+16] ^= 0xff
		ref.SHA256 = sha256Bytes(blob)
		bucket := "cluster/balance-history/nodes/node-1/runs/" + ref.Hex()
		cold.seed(bucket, archiveChapterID, blob, ref.SHA256[:])
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		_, err := store.Fetch(context.Background(), ref)
		require.ErrorIs(t, err, ErrCorrupt)
	})
}

func TestCacheHitAvoidsSecondColdFetch(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, ref := seedRun(t, cold, "cache-hit", 128)
	store := newTestStore(t, cold, t.TempDir(), 1<<20)

	for range 2 {
		lease, err := store.Fetch(context.Background(), ref)
		require.NoError(t, err)
		require.NoError(t, lease.Close())
	}
	require.Equal(t, 1, cold.fetchCount())
}

func TestAvailableIsHeadOnlyAndDistinguishesIntegrityState(t *testing.T) {
	t.Parallel()

	t.Run("available", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		_, ref := seedRun(t, cold, "available", 64)
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		available, err := store.Available(context.Background(), ref)
		require.NoError(t, err)
		require.True(t, available)
		require.Equal(t, 0, cold.fetchCount())
		require.Equal(t, 0, cold.checksumCount())
	})

	t.Run("object missing", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		_, ref := encodeBlob(t, testRecords("unavailable", 64))
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		available, err := store.Available(context.Background(), ref)
		require.NoError(t, err)
		require.False(t, available)
		require.Equal(t, 0, cold.fetchCount())
		require.Equal(t, 0, cold.checksumCount())
	})

	t.Run("checksum metadata missing", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		blob, ref := encodeBlob(t, testRecords("missing-metadata", 64))
		cold.seed("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID, blob, nil)
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		available, err := store.Available(context.Background(), ref)
		require.NoError(t, err)
		require.False(t, available)
		require.Equal(t, 0, cold.fetchCount())
		require.Equal(t, 0, cold.checksumCount())
	})

	t.Run("checksum metadata mismatch", func(t *testing.T) {
		t.Parallel()

		cold := newMemoryColdStorage(t)
		blob, ref := encodeBlob(t, testRecords("metadata-mismatch", 64))
		cold.seed(
			"cluster/balance-history/nodes/node-1/runs/"+ref.Hex(),
			archiveChapterID,
			blob,
			bytes.Repeat([]byte{0xee}, sha256.Size),
		)
		store := newTestStore(t, cold, t.TempDir(), 1<<20)

		available, err := store.Available(context.Background(), ref)
		require.ErrorIs(t, err, ErrCorrupt)
		require.False(t, available)
		require.Equal(t, 0, cold.fetchCount())
		require.Equal(t, 0, cold.checksumCount())
	})
}

func TestCorruptCacheHitIsRejectedBeforeReadingAndRemoved(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, ref := seedRun(t, cold, "cache-corrupt", 128)
	cacheDir := t.TempDir()
	store := newTestStore(t, cold, cacheDir, 1<<20)

	lease, err := store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	require.NoError(t, lease.Close())

	path := filepath.Join(cacheDir, ref.Hex()+cacheFileSuffix)
	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	encoded[len(encoded)/2] ^= 0xff
	require.NoError(t, os.Chmod(path, 0o600))
	require.NoError(t, os.WriteFile(path, encoded, 0o600))

	lease, err = store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	_, err = lease.Open()
	require.ErrorIs(t, err, ErrCorrupt)
	require.Equal(t, CacheStats{}, store.CacheStats())

	lease, err = store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	require.Equal(t, testRecords("cache-corrupt", 128), readLease(t, lease))
	require.Equal(t, 2, cold.fetchCount())
}

func TestConcurrentCacheMissUsesOneColdFetch(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, ref := seedRun(t, cold, "singleflight", 4096)
	started := make(chan struct{})
	release := make(chan struct{})
	cold.blockFetch(started, release)
	store := newTestStore(t, cold, t.TempDir(), 1<<20)

	const readers = 16
	start := make(chan struct{})
	errorsByReader := make(chan error, readers)
	var waitGroup sync.WaitGroup
	for range readers {
		waitGroup.Go(func() {
			<-start
			lease, err := store.Fetch(context.Background(), ref)
			if err == nil {
				err = lease.Close()
			}
			errorsByReader <- err
		})
	}
	close(start)
	<-started
	close(release)
	waitGroup.Wait()
	close(errorsByReader)
	for err := range errorsByReader {
		require.NoError(t, err)
	}
	require.Equal(t, 1, cold.fetchCount())
}

func TestCacheEvictsLeastRecentlyUsedBytes(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, first := seedRun(t, cold, "evict-a", 512)
	_, second := seedRun(t, cold, "evict-b", 512)
	maxBytes := int64(max(first.Size, second.Size) + 1)
	store := newTestStore(t, cold, t.TempDir(), maxBytes)

	lease, err := store.Fetch(context.Background(), first)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	lease, err = store.Fetch(context.Background(), second)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	lease, err = store.Fetch(context.Background(), first)
	require.NoError(t, err)
	require.NoError(t, lease.Close())

	require.Equal(t, 3, cold.fetchCount())
	require.LessOrEqual(t, store.CacheStats().Bytes, maxBytes)
}

func TestLeasePreventsEviction(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, first := seedRun(t, cold, "leased-a", 512)
	_, second := seedRun(t, cold, "leased-b", 512)
	maxBytes := int64(max(first.Size, second.Size) + 1)
	store := newTestStore(t, cold, t.TempDir(), maxBytes)

	firstLease, err := store.Fetch(context.Background(), first)
	require.NoError(t, err)
	secondLease, err := store.Fetch(context.Background(), second)
	require.NoError(t, err)
	require.Equal(t, CacheStats{
		Bytes:   int64(first.Size + second.Size),
		Entries: 2,
		Leases:  2,
	}, store.CacheStats())

	require.NoError(t, secondLease.Close())
	require.Equal(t, 1, store.CacheStats().Entries)
	additionalLease, err := store.Fetch(context.Background(), first)
	require.NoError(t, err)
	require.NoError(t, additionalLease.Close())
	require.Equal(t, 2, cold.fetchCount())
	require.NoError(t, firstLease.Close())
}

func TestStartupIgnoresCrashTemporaryFiles(t *testing.T) {
	t.Parallel()

	cold := newMemoryColdStorage(t)
	_, ref := seedRun(t, cold, "crash-temp", 64)
	cacheDir := t.TempDir()
	temporaryPath := filepath.Join(cacheDir, ref.Hex()+cacheFileSuffix+".tmp")
	require.NoError(t, os.WriteFile(temporaryPath, bytes.Repeat([]byte("partial"), 50), 0o600))

	store := newTestStore(t, cold, cacheDir, 1<<20)
	require.Equal(t, CacheStats{}, store.CacheStats())
	lease, err := store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	require.Equal(t, 1, cold.fetchCount())
	require.FileExists(t, temporaryPath)
}

func newTestStore(t *testing.T, cold *memoryColdStorageFixture, cacheDir string, maxBytes int64) *Store {
	t.Helper()

	store, err := New(cold.mock, Config{
		BaseBucketID:  "cluster",
		OwnerID:       "node-1",
		CacheDir:      cacheDir,
		CacheMaxBytes: maxBytes,
	}, noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func testRecords(prefix string, valueSize int) []Record {
	return []Record{
		{Key: []byte(prefix + "/001"), Value: bytes.Repeat([]byte{0x01}, valueSize)},
		{Key: []byte(prefix + "/002"), Value: bytes.Repeat([]byte{0x02}, valueSize)},
	}
}

func encodeBlob(t *testing.T, records []Record) ([]byte, Ref) {
	t.Helper()

	var encoded bytes.Buffer
	ref, err := Encode(context.Background(), &encoded, NewSliceStream(records))
	require.NoError(t, err)

	return encoded.Bytes(), ref
}

func sha256Bytes(data []byte) [sha256.Size]byte {
	return sha256.Sum256(data)
}

func seedRun(t *testing.T, cold *memoryColdStorageFixture, prefix string, valueSize int) ([]Record, Ref) {
	t.Helper()

	records := testRecords(prefix, valueSize)
	blob, ref := encodeBlob(t, records)
	cold.seed("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID, blob, ref.SHA256[:])

	return records, ref
}

func readLease(t *testing.T, lease *Lease) []Record {
	t.Helper()
	defer func() { require.NoError(t, lease.Close()) }()

	reader, err := lease.Open()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	var records []Record
	for reader.Next() {
		record := reader.Record()
		records = append(records, Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)})
	}
	require.NoError(t, reader.Err())

	return records
}

type memoryColdObject struct {
	data     []byte
	expected []byte
}

type memoryColdStorageFixture struct {
	mu           sync.Mutex
	objects      map[string]memoryColdObject
	archives     int
	fetches      int
	checksums    int
	fetchStarted chan struct{}
	fetchRelease chan struct{}
	startOnce    sync.Once
	mock         *MockIdentifiedStorage
}

func newMemoryColdStorage(t *testing.T) *memoryColdStorageFixture {
	t.Helper()

	cold := &memoryColdStorageFixture{objects: make(map[string]memoryColdObject)}
	cold.mock = NewMockIdentifiedStorage(gomock.NewController(t))
	cold.mock.EXPECT().DestinationIdentity().AnyTimes().Return("test-memory-cold-storage-v1", nil)
	cold.mock.EXPECT().Archive(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(cold.archive)
	cold.mock.EXPECT().Exists(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(cold.exists)
	cold.mock.EXPECT().ExpectedChecksum(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(cold.expectedChecksum)
	cold.mock.EXPECT().Checksum(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(cold.checksum)
	cold.mock.EXPECT().Fetch(
		gomock.Any(), gomock.Any(), gomock.Any(),
	).AnyTimes().DoAndReturn(cold.fetch)

	return cold
}

func (s *memoryColdStorageFixture) archive(
	_ context.Context,
	bucketID string,
	chapterID uint64,
	data io.Reader,
	expected []byte,
) error {
	encoded, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	calculated := sha256.Sum256(encoded)
	if !bytes.Equal(calculated[:], expected) {
		return errors.New("test archive checksum mismatch")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.archives++
	s.objects[memoryColdKey(bucketID, chapterID)] = memoryColdObject{
		data:     bytes.Clone(encoded),
		expected: bytes.Clone(expected),
	}

	return nil
}

func (s *memoryColdStorageFixture) exists(_ context.Context, bucketID string, chapterID uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.objects[memoryColdKey(bucketID, chapterID)]

	return ok, nil
}

func (s *memoryColdStorageFixture) expectedChecksum(
	_ context.Context,
	bucketID string,
	chapterID uint64,
) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	object, ok := s.objects[memoryColdKey(bucketID, chapterID)]
	if !ok {
		return nil, coldstorage.ErrChecksumNotFound
	}
	if len(object.expected) == 0 {
		return nil, coldstorage.ErrChecksumNotFound
	}

	return bytes.Clone(object.expected), nil
}

func (s *memoryColdStorageFixture) checksum(_ context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checksums++

	object, ok := s.objects[memoryColdKey(bucketID, chapterID)]
	if !ok {
		return nil, os.ErrNotExist
	}
	digest := sha256.Sum256(object.data)

	return digest[:], nil
}

func (s *memoryColdStorageFixture) fetch(_ context.Context, bucketID string, chapterID uint64) (io.ReadCloser, error) {
	s.mu.Lock()
	object, ok := s.objects[memoryColdKey(bucketID, chapterID)]
	s.fetches++
	started := s.fetchStarted
	release := s.fetchRelease
	s.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	if started != nil {
		s.startOnce.Do(func() { close(started) })
	}
	if release != nil {
		<-release
	}

	return io.NopCloser(bytes.NewReader(bytes.Clone(object.data))), nil
}

func (s *memoryColdStorageFixture) seed(bucketID string, chapterID uint64, data, expected []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[memoryColdKey(bucketID, chapterID)] = memoryColdObject{
		data:     bytes.Clone(data),
		expected: bytes.Clone(expected),
	}
}

func (s *memoryColdStorageFixture) corruptData(bucketID string, chapterID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := memoryColdKey(bucketID, chapterID)
	object := s.objects[key]
	object.data[len(object.data)/2] ^= 0xff
	s.objects[key] = object
}

func (s *memoryColdStorageFixture) blockFetch(started, release chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fetchStarted = started
	s.fetchRelease = release
}

func (s *memoryColdStorageFixture) archiveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.archives
}

func (s *memoryColdStorageFixture) fetchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.fetches
}

func (s *memoryColdStorageFixture) checksumCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.checksums
}

func (s *memoryColdStorageFixture) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

func memoryColdKey(bucketID string, chapterID uint64) string {
	return fmt.Sprintf("%s#%d", bucketID, chapterID)
}
