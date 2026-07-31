package coldstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// makeChecksum produces a deterministic, valid-length checksum from a seed
// byte. Used to keep test payloads short and readable.
func makeChecksum(seed byte) []byte {
	b := make([]byte, ChecksumLength)
	for i := range b {
		b[i] = seed
	}

	return b
}

func sha256Of(t *testing.T, content string) []byte {
	t.Helper()
	c, err := ComputeSHA256(strings.NewReader(content))
	require.NoError(t, err)

	return c
}

func dataPath(dir, bucket string, chapterID uint64) string {
	return filepath.Join(dir, bucket, "chapters", strconv.FormatUint(chapterID, 10), archiveDataName)
}

func sidecarPath(dir, bucket string, chapterID uint64) string {
	return filepath.Join(dir, bucket, "chapters", strconv.FormatUint(chapterID, 10), archiveChecksumName)
}

func TestFilesystemStorage_ArchiveAndExists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	checksum := sha256Of(t, "archive content here")
	err := fs.Archive(ctx, "bucket1", 42, strings.NewReader("archive content here"), checksum)
	require.NoError(t, err)

	exists, err := fs.Exists(ctx, "bucket1", 42)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemStorage_ExistsNotFound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	exists, err := fs.Exists(ctx, "bucket1", 999)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestFilesystemStorage_ArchiveCreatesDirectories(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	checksum := sha256Of(t, "deep archive")
	err := fs.Archive(ctx, "deep-bucket", 1, strings.NewReader("deep archive"), checksum)
	require.NoError(t, err)

	exists, err := fs.Exists(ctx, "deep-bucket", 1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemStorage_ArchiveOverwrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	// First version
	c1 := sha256Of(t, "version1")
	require.NoError(t, fs.Archive(ctx, "bucket", 1, strings.NewReader("version1"), c1))

	// Overwrite
	c2 := sha256Of(t, "version2")
	require.NoError(t, fs.Archive(ctx, "bucket", 1, strings.NewReader("version2"), c2))

	rc, err := fs.Fetch(ctx, "bucket", 1)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	data, _ := io.ReadAll(rc)
	require.Equal(t, "version2", string(data))

	expected, err := fs.ExpectedChecksum(ctx, "bucket", 1)
	require.NoError(t, err)
	require.True(t, bytes.Equal(c2, expected), "checksum sidecar must reflect the latest write")
}

func TestFilesystemStorage_ArchiveReadBack(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	content := "archive data for read-back test"
	checksum := sha256Of(t, content)
	require.NoError(t, fs.Archive(ctx, "bucket", 7, strings.NewReader(content), checksum))

	rc, err := fs.Fetch(ctx, "bucket", 7)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

func TestFilesystemStorage_LargeArchive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	largeData := strings.Repeat("x", 100000)
	checksum := sha256Of(t, largeData)
	require.NoError(t, fs.Archive(ctx, "bucket", 99, strings.NewReader(largeData), checksum))

	exists, err := fs.Exists(ctx, "bucket", 99)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemStorage_DifferentChapters(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	c1 := sha256Of(t, "chapter-1")
	c2 := sha256Of(t, "chapter-2")
	require.NoError(t, fs.Archive(ctx, "bucket", 1, strings.NewReader("chapter-1"), c1))
	require.NoError(t, fs.Archive(ctx, "bucket", 2, strings.NewReader("chapter-2"), c2))

	exists1, err := fs.Exists(ctx, "bucket", 1)
	require.NoError(t, err)
	require.True(t, exists1)

	exists2, err := fs.Exists(ctx, "bucket", 2)
	require.NoError(t, err)
	require.True(t, exists2)

	exists3, err := fs.Exists(ctx, "bucket", 3)
	require.NoError(t, err)
	require.False(t, exists3)
}

func TestFilesystemStorage_DifferentBuckets(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	checksum := sha256Of(t, "data-a")
	require.NoError(t, fs.Archive(ctx, "bucket-a", 1, strings.NewReader("data-a"), checksum))

	exists, err := fs.Exists(ctx, "bucket-b", 1)
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = fs.Exists(ctx, "bucket-a", 1)
	require.NoError(t, err)
	require.True(t, exists)
}

// --- Integrity-focused tests ---

func TestFilesystemStorage_ArchivePersistsChecksum(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	expected := makeChecksum(0xAB)
	require.NoError(t, fs.Archive(ctx, "bucket", 1, strings.NewReader("some bytes"), expected))

	// Both files must exist
	_, err := os.Stat(dataPath(dir, "bucket", 1))
	require.NoError(t, err, "archive.sst must be written")

	got, err := os.ReadFile(sidecarPath(dir, "bucket", 1))
	require.NoError(t, err, "sidecar must be written")
	require.Equal(t, expected, got, "sidecar must contain exact checksum bytes")
}

func TestFilesystemStorage_ExistsRequiresBothFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	chapterDir := filepath.Join(dir, "bucket", "chapters", "5")
	require.NoError(t, os.MkdirAll(chapterDir, 0o755))

	// Only data, no sidecar
	require.NoError(t, os.WriteFile(filepath.Join(chapterDir, archiveDataName), []byte("orphan"), 0o644))
	exists, err := fs.Exists(ctx, "bucket", 5)
	require.NoError(t, err)
	require.False(t, exists, "data alone must not count as committed")

	// Only sidecar, no data
	require.NoError(t, os.Remove(filepath.Join(chapterDir, archiveDataName)))
	require.NoError(t, os.WriteFile(filepath.Join(chapterDir, archiveChecksumName), makeChecksum(0x01), 0o644))
	exists, err = fs.Exists(ctx, "bucket", 5)
	require.NoError(t, err)
	require.False(t, exists, "sidecar alone must not count as committed")

	// Both present
	require.NoError(t, os.WriteFile(filepath.Join(chapterDir, archiveDataName), []byte("orphan"), 0o644))
	exists, err = fs.Exists(ctx, "bucket", 5)
	require.NoError(t, err)
	require.True(t, exists, "data + sidecar means committed")
}

func TestFilesystemStorage_ExpectedChecksumReadsFromSidecar(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	expected := makeChecksum(0x42)
	require.NoError(t, fs.Archive(ctx, "bucket", 3, strings.NewReader("x"), expected))

	got, err := fs.ExpectedChecksum(ctx, "bucket", 3)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestFilesystemStorage_ExpectedChecksumMissingReturnsSentinel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	_, err := fs.ExpectedChecksum(ctx, "bucket", 99)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrChecksumNotFound),
		"missing sidecar must surface as ErrChecksumNotFound so callers can re-upload")
}

func TestFilesystemStorage_MalformedSidecar(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	chapterDir := filepath.Join(dir, "bucket", "chapters", "8")
	require.NoError(t, os.MkdirAll(chapterDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(chapterDir, archiveDataName), []byte("data"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(chapterDir, archiveChecksumName), []byte("not-32-bytes"), 0o644))

	_, err := fs.ExpectedChecksum(ctx, "bucket", 8)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrChecksumMalformed),
		"wrong-length sidecar must surface as ErrChecksumMalformed, not be silently accepted")
}

func TestFilesystemStorage_ChecksumIsContentBased(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	original := "original bytes"
	expected := sha256Of(t, original)
	require.NoError(t, fs.Archive(ctx, "bucket", 11, strings.NewReader(original), expected))

	// Tamper with the data on disk (sidecar untouched).
	require.NoError(t, os.WriteFile(dataPath(dir, "bucket", 11), []byte("tampered bytes"), 0o644))

	got, err := fs.Checksum(ctx, "bucket", 11)
	require.NoError(t, err)
	require.NotEqual(t, expected, got, "Checksum must reflect current bytes, not the sidecar")

	stored, err := fs.ExpectedChecksum(ctx, "bucket", 11)
	require.NoError(t, err)
	require.Equal(t, expected, stored, "ExpectedChecksum must reflect the sidecar, unchanged after tampering")
}

func TestFilesystemStorage_NoOrphanTmpAfterArchive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	require.NoError(t, fs.Archive(ctx, "bucket", 4, strings.NewReader("x"), makeChecksum(0x07)))

	entries, err := os.ReadDir(filepath.Join(dir, "bucket", "chapters", "4"))
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), ".tmp"),
			"successful Archive must not leave .tmp files: %s", e.Name())
	}
}

func TestFilesystemStorage_RejectsWrongChecksumLength(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	err := fs.Archive(ctx, "bucket", 1, strings.NewReader("x"), []byte{0x01, 0x02})
	require.Error(t, err, "Archive must reject a checksum that is not the SHA-256 size")
}

func TestFilesystemStorageObjectCatalogPaginatesWithinPrefix(t *testing.T) {
	t.Parallel()

	storage := NewFilesystemStorage(t.TempDir())
	ctx := context.Background()
	prefix := "cluster/balance-history/nodes/node-1/runs"
	wantBuckets := []string{
		prefix + "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		prefix + "/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		prefix + "/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	for index, bucketID := range wantBuckets {
		content := "owned-" + strconv.Itoa(index)
		require.NoError(t, storage.Archive(ctx, bucketID, 0, strings.NewReader(content), sha256Of(t, content)))
	}
	foreign := "cluster/balance-history/nodes/node-2/runs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, storage.Archive(ctx, foreign, 0, strings.NewReader("foreign"), sha256Of(t, "foreign")))

	var (
		cursor string
		got    []ArchiveObject
	)
	for {
		page, err := storage.List(ctx, prefix, cursor, 2)
		require.NoError(t, err)
		got = append(got, page.Objects...)
		if page.NextCursor == "" {
			break
		}
		require.NotEqual(t, cursor, page.NextCursor)
		cursor = page.NextCursor
	}
	require.Len(t, got, len(wantBuckets))
	for index, object := range got {
		require.Equal(t, wantBuckets[index], object.BucketID)
		require.Zero(t, object.ChapterID)
		require.Positive(t, object.Size)
		require.False(t, object.LastModified.IsZero())
	}
}

func TestFilesystemStorageObjectCatalogListsIncompleteObject(t *testing.T) {
	t.Parallel()

	storage := NewFilesystemStorage(t.TempDir())
	ctx := context.Background()
	prefix := "cluster/balance-history/nodes/node-1/runs"
	bucketID := prefix + "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, storage.Archive(ctx, bucketID, 0, strings.NewReader("incomplete"), sha256Of(t, "incomplete")))
	require.NoError(t, os.Remove(storage.checksumPath(bucketID, 0)))

	exists, err := storage.Exists(ctx, bucketID, 0)
	require.NoError(t, err)
	require.False(t, exists)
	page, err := storage.List(ctx, prefix, "", 10)
	require.NoError(t, err)
	require.Equal(t, []ArchiveObject{{
		BucketID:     bucketID,
		ChapterID:    0,
		Size:         int64(len("incomplete")),
		LastModified: page.Objects[0].LastModified,
	}}, page.Objects)
}

func TestFilesystemStorageObjectCatalogDeleteIsIdempotentAndScoped(t *testing.T) {
	t.Parallel()

	storage := NewFilesystemStorage(t.TempDir())
	ctx := context.Background()
	bucketID := "cluster/balance-history/nodes/node-1/runs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sibling := "cluster/balance-history/nodes/node-1/runs/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, target := range []struct {
		bucketID  string
		chapterID uint64
		content   string
	}{
		{bucketID: bucketID, chapterID: 0, content: "delete-me"},
		{bucketID: bucketID, chapterID: 1, content: "keep-chapter"},
		{bucketID: sibling, chapterID: 0, content: "keep-sibling"},
	} {
		require.NoError(t, storage.Archive(ctx, target.bucketID, target.chapterID, strings.NewReader(target.content), sha256Of(t, target.content)))
	}

	require.NoError(t, storage.Delete(ctx, bucketID, 0))
	require.NoError(t, storage.Delete(ctx, bucketID, 0), "delete must be idempotent")
	exists, err := storage.Exists(ctx, bucketID, 0)
	require.NoError(t, err)
	require.False(t, exists)
	for _, retained := range []struct {
		bucketID  string
		chapterID uint64
	}{{bucketID: bucketID, chapterID: 1}, {bucketID: sibling, chapterID: 0}} {
		exists, err := storage.Exists(ctx, retained.bucketID, retained.chapterID)
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func TestFilesystemStorageObjectCatalogDeleteRetriesPartialState(t *testing.T) {
	t.Parallel()

	storage := NewFilesystemStorage(t.TempDir())
	ctx := context.Background()
	prefix := "cluster/balance-history/nodes/node-1/runs"
	dataOnly := prefix + "/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	checksumOnly := prefix + "/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	for _, bucketID := range []string{dataOnly, checksumOnly} {
		require.NoError(t, storage.Archive(ctx, bucketID, 0, strings.NewReader(bucketID), sha256Of(t, bucketID)))
	}
	require.NoError(t, os.Remove(storage.checksumPath(dataOnly, 0)))
	require.NoError(t, os.Remove(storage.archivePath(checksumOnly, 0)))

	require.NoError(t, storage.Delete(ctx, dataOnly, 0))
	require.NoError(t, storage.Delete(ctx, checksumOnly, 0))
	require.NoFileExists(t, storage.archivePath(dataOnly, 0))
	require.NoFileExists(t, storage.checksumPath(dataOnly, 0))
	require.NoFileExists(t, storage.archivePath(checksumOnly, 0))
	require.NoFileExists(t, storage.checksumPath(checksumOnly, 0))
}
