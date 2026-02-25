package coldstorage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilesystemStorage_ArchiveAndExists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	data := strings.NewReader("archive content here")
	err := fs.Archive(ctx, "bucket1", 42, data)
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

	// Deep nesting should auto-create dirs
	data := strings.NewReader("deep archive")
	err := fs.Archive(ctx, "deep-bucket", 1, data)
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

	// Write first version
	err := fs.Archive(ctx, "bucket", 1, strings.NewReader("version1"))
	require.NoError(t, err)

	// Overwrite
	err = fs.Archive(ctx, "bucket", 1, strings.NewReader("version2"))
	require.NoError(t, err)

	exists, err := fs.Exists(ctx, "bucket", 1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemStorage_ArchiveReadBack(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	content := "archive data for read-back test"
	err := fs.Archive(ctx, "bucket", 7, strings.NewReader(content))
	require.NoError(t, err)

	// Read back the file directly to verify content
	path := filepath.Join(dir, "bucket", "periods", "7", "archive.tar.gz")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

func TestFilesystemStorage_LargeArchive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	// Write a large piece of data
	largeData := strings.Repeat("x", 100000)
	err := fs.Archive(ctx, "bucket", 99, strings.NewReader(largeData))
	require.NoError(t, err)

	exists, err := fs.Exists(ctx, "bucket", 99)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestFilesystemStorage_DifferentPeriods(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	fs := NewFilesystemStorage(dir)
	ctx := context.Background()

	require.NoError(t, fs.Archive(ctx, "bucket", 1, strings.NewReader("period-1")))
	require.NoError(t, fs.Archive(ctx, "bucket", 2, strings.NewReader("period-2")))

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

	err := fs.Archive(ctx, "bucket-a", 1, strings.NewReader("data-a"))
	require.NoError(t, err)

	// Different bucket, same period
	exists, err := fs.Exists(ctx, "bucket-b", 1)
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = fs.Exists(ctx, "bucket-a", 1)
	require.NoError(t, err)
	require.True(t, exists)
}
