package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// stubHashCounter swaps hashFileFn for one that records the base name of every
// file it hashes (then delegates to the real hasher), returning a restore func.
// Tests using it must NOT run in parallel: hashFileFn is a package global.
func stubHashCounter(hashed *[]string) func() {
	orig := hashFileFn
	hashFileFn = func(ctx context.Context, path string) (string, error) {
		*hashed = append(*hashed, filepath.Base(path))

		return orig(ctx, path)
	}

	return func() { hashFileFn = orig }
}

func writeCheckpointFile(t *testing.T, dir, name, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
}

// TestListCheckpointFiles_ReusesUnchangedImmutableFiles verifies that an
// unchanged .sst/.blob (same name+size+mtime, key still present remotely) is
// NOT re-hashed on a subsequent run, while metadata files are always hashed.
func TestListCheckpointFiles_ReusesUnchangedImmutableFiles(t *testing.T) {
	// No t.Parallel(): mutates the hashFileFn global.
	dir := t.TempDir()
	writeCheckpointFile(t, dir, "100.sst", "sst-contents")
	writeCheckpointFile(t, dir, "200.blob", "blob-contents")
	writeCheckpointFile(t, dir, "MANIFEST-000001", "manifest-contents")
	writeCheckpointFile(t, dir, "CURRENT", "MANIFEST-000001\n")

	ctx := logging.TestingContext()

	// First pass: no prior manifest → every file is hashed.
	var firstHashed []string
	restore := stubHashCounter(&firstHashed)
	files1, err := listCheckpointFiles(ctx, "bucket", dir, &Manifest{}, nil)
	restore()
	require.NoError(t, err)
	require.Len(t, files1, 4)
	require.ElementsMatch(t, []string{"100.sst", "200.blob", "MANIFEST-000001", "CURRENT"}, firstHashed)

	// Prior manifest = the first pass output; mark the immutable keys present remotely.
	prev := &Manifest{Checkpoint: &CheckpointManifest{Files: files1}}
	existing := map[string]struct{}{
		files1["100.sst"].Key:  {},
		files1["200.blob"].Key: {},
	}

	// Second pass: .sst/.blob reused (not hashed); metadata files re-hashed.
	var secondHashed []string
	restore = stubHashCounter(&secondHashed)
	files2, err := listCheckpointFiles(ctx, "bucket", dir, prev, existing)
	restore()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"MANIFEST-000001", "CURRENT"}, secondHashed,
		"only mutable-name metadata files should be re-hashed")
	require.Equal(t, files1["100.sst"].Key, files2["100.sst"].Key, "reused key must be identical")
	require.Equal(t, files1["200.blob"].Key, files2["200.blob"].Key)
}

// TestListCheckpointFiles_ReHashesWhenRemoteObjectMissing is the critical
// correctness guard: a prior key whose object is NOT present remotely must be
// re-hashed, never reused (reuse would record a stale hash for current bytes).
func TestListCheckpointFiles_ReHashesWhenRemoteObjectMissing(t *testing.T) {
	dir := t.TempDir()
	writeCheckpointFile(t, dir, "100.sst", "sst-contents")

	ctx := logging.TestingContext()

	files1, err := listCheckpointFiles(ctx, "bucket", dir, &Manifest{}, nil)
	require.NoError(t, err)

	prev := &Manifest{Checkpoint: &CheckpointManifest{Files: files1}}

	// existingKeys is empty → object missing remotely → must re-hash.
	var hashed []string
	restore := stubHashCounter(&hashed)
	_, err = listCheckpointFiles(ctx, "bucket", dir, prev, map[string]struct{}{})
	restore()
	require.NoError(t, err)
	require.Contains(t, hashed, "100.sst", "must re-hash when the prior key is absent remotely")
}

// TestListCheckpointFiles_ReHashesOnSizeChange verifies a size change defeats reuse.
func TestListCheckpointFiles_ReHashesOnSizeChange(t *testing.T) {
	dir := t.TempDir()
	writeCheckpointFile(t, dir, "100.sst", "sst-contents")

	ctx := logging.TestingContext()
	files1, err := listCheckpointFiles(ctx, "bucket", dir, &Manifest{}, nil)
	require.NoError(t, err)

	// Prior manifest records a different size for the same name.
	stale := files1["100.sst"]
	stale.Size += 1
	prev := &Manifest{Checkpoint: &CheckpointManifest{Files: map[string]CheckpointFile{"100.sst": stale}}}
	existing := map[string]struct{}{stale.Key: {}}

	var hashed []string
	restore := stubHashCounter(&hashed)
	_, err = listCheckpointFiles(ctx, "bucket", dir, prev, existing)
	restore()
	require.NoError(t, err)
	require.Contains(t, hashed, "100.sst", "size mismatch must force a re-hash")
}

// flakyStorage fails PutFile a fixed number of times, then succeeds. Only
// PutFile is exercised by the retry helpers; the rest satisfy the interface.
type flakyStorage struct {
	failsLeft int
	calls     int
}

func (f *flakyStorage) PutFile(_ context.Context, _ string, data io.Reader, _ int64) error {
	f.calls++
	_, _ = io.Copy(io.Discard, data) // drain so a pipe body unblocks

	if f.failsLeft > 0 {
		f.failsLeft--

		return errors.New("transient upload error")
	}

	return nil
}

func (f *flakyStorage) GetFile(context.Context, string) (io.ReadCloser, error) {
	return nil, ErrFileNotFound
}
func (f *flakyStorage) DeleteFile(context.Context, string) error            { return nil }
func (f *flakyStorage) ListFiles(context.Context, string) ([]string, error) { return nil, nil }

// withFastBackoff shrinks the retry backoff so retry tests run quickly.
func withFastBackoff(t *testing.T) {
	t.Helper()
	origInit, origMax := uploadInitialBackoff, uploadMaxBackoff
	uploadInitialBackoff = time.Millisecond
	uploadMaxBackoff = 2 * time.Millisecond
	t.Cleanup(func() {
		uploadInitialBackoff = origInit
		uploadMaxBackoff = origMax
	})
}

func TestPutWithRetry_SucceedsAfterTransientFailures(t *testing.T) {
	withFastBackoff(t)

	storage := &flakyStorage{failsLeft: 2}

	var cleanups int
	err := putWithRetry(context.Background(), storage, "k", 3, logging.Testing(), func() (io.Reader, func(), error) {
		return bytes.NewReader([]byte("abc")), func() { cleanups++ }, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, storage.calls, "should retry twice then succeed")
	require.Equal(t, 3, cleanups, "cleanup must run once per attempt")
}

func TestPutWithRetry_ExhaustsAttempts(t *testing.T) {
	withFastBackoff(t)

	storage := &flakyStorage{failsLeft: 100}
	err := putWithRetry(context.Background(), storage, "k", 0, logging.Testing(), func() (io.Reader, func(), error) {
		return bytes.NewReader([]byte("abc")), func() {}, nil
	})
	require.Error(t, err)
	require.Equal(t, uploadMaxAttempts, storage.calls)
}

func TestPutWithRetry_NoRetryOnCanceledContext(t *testing.T) {
	withFastBackoff(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	storage := &flakyStorage{failsLeft: 100}
	err := putWithRetry(ctx, storage, "k", 0, logging.Testing(), func() (io.Reader, func(), error) {
		return bytes.NewReader([]byte("abc")), func() {}, nil
	})
	require.Error(t, err)
	require.Equal(t, 1, storage.calls, "a cancelled context must not be retried")
}

// TestListCheckpointFiles_ReHashesOnMtimeChange verifies that a size-identical
// file whose mtime differs from the prior manifest is re-hashed — mtime alone
// must defeat reuse (it is the store-lineage discriminator).
func TestListCheckpointFiles_ReHashesOnMtimeChange(t *testing.T) {
	dir := t.TempDir()
	writeCheckpointFile(t, dir, "100.sst", "sst-contents")

	ctx := logging.TestingContext()
	files1, err := listCheckpointFiles(ctx, "bucket", dir, &Manifest{}, nil)
	require.NoError(t, err)

	// Same name, same size, but the prior manifest recorded an older mtime.
	stale := files1["100.sst"]
	stale.ModTimeUnixNano -= int64(time.Second)
	require.Equal(t, files1["100.sst"].Size, stale.Size, "guard: size must be identical for this test")

	prev := &Manifest{Checkpoint: &CheckpointManifest{Files: map[string]CheckpointFile{"100.sst": stale}}}
	existing := map[string]struct{}{stale.Key: {}}

	var hashed []string
	restore := stubHashCounter(&hashed)
	_, err = listCheckpointFiles(ctx, "bucket", dir, prev, existing)
	restore()
	require.NoError(t, err)
	require.Contains(t, hashed, "100.sst", "mtime mismatch with identical size must force a re-hash")
}

// partialFailOnceStorage wraps a working in-memory storage but, the first time
// each key is uploaded, reads a few bytes off the body then returns an error —
// simulating a transient failure after a partial pipe read. The retry must
// re-seek the iterator and re-stream the segment from scratch.
type partialFailOnceStorage struct {
	*inMemoryBackupStorage

	failed map[string]bool
	calls  int
}

func (s *partialFailOnceStorage) PutFile(ctx context.Context, key string, data io.Reader, size int64) error {
	s.calls++

	if !s.failed[key] {
		s.failed[key] = true

		buf := make([]byte, 4)
		_, _ = data.Read(buf) // partial read, then abandon

		return errors.New("simulated transient upload failure after partial read")
	}

	return s.inMemoryBackupStorage.PutFile(ctx, key, data, size)
}

// TestExportEntries_RetriesStreamedSegmentAndRoundTrips exercises the streaming
// (io.Pipe) retry path: the first PutFile for the segment fails after a partial
// read, the retry re-seeks the Pebble iterator to the segment start and
// re-streams, and applying the result reconstructs every entry — proving the
// re-seek replays the exact same bytes.
func TestExportEntries_RetriesStreamedSegmentAndRoundTrips(t *testing.T) {
	withFastBackoff(t)

	ctx := logging.TestingContext()
	src := newBackupTestStore(t)

	const n = 5

	want := make(map[uint64][]byte, n)

	batch := src.OpenWriteSession()
	for seq := uint64(1); seq <= n; seq++ {
		val := bytes.Repeat([]byte{byte(seq)}, 256)
		require.NoError(t, batch.SetBytes(coldLogKey(seq), val))
		want[seq] = val
	}

	require.NoError(t, batch.Commit())

	storage := &partialFailOnceStorage{
		inMemoryBackupStorage: newInMemoryBackupStorage(),
		failed:                map[string]bool{},
	}

	readHandle, err := src.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = readHandle.Close() })

	// Large cap → a single segment, so exactly one key is uploaded (fail + retry).
	segs, count, err := exportEntries(ctx, logging.Testing(), storage, readHandle,
		dal.ZoneCold, dal.SubColdLog, 0, n, "log",
		func(part int) string { return ExportLogSegmentKey("bucket", 1, n, part) },
		1<<30,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(n), count)
	require.Len(t, segs, 1)
	require.Equal(t, 2, storage.calls, "segment upload must fail once then succeed on retry")

	// Round-trip: applying the retried segment reconstructs every entry, proving
	// the iterator re-seek replayed the identical range.
	dst := newBackupTestStore(t)
	require.NoError(t, ApplyExports(ctx, logging.Testing(), storage, dst, segs))

	dstHandle, err := dst.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstHandle.Close() })

	for seq := uint64(1); seq <= n; seq++ {
		got, closer, err := dstHandle.Get(coldLogKey(seq))
		require.NoError(t, err, "seq %d", seq)
		require.Equal(t, want[seq], got, "seq %d", seq)
		_ = closer.Close()
	}
}
