package ctrl

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
)

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)

	return hex.EncodeToString(h[:])
}

func TestScanCompletedFiles_MatchesManifest(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("hello world")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), content, 0644))

	manifest := &snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.txt", Size: uint64(len(content)), Sha256: sha256Hex(content)},
			{Path: "b.txt", Size: 5, Sha256: "deadbeef"},
		},
	}

	completed, err := scanCompletedFiles(dir, manifest)
	require.NoError(t, err)
	require.Equal(t, []string{"a.txt"}, completed)
}

func TestScanCompletedFiles_SizeMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("hello world")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), content, 0644))

	manifest := &snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.txt", Size: 999, Sha256: sha256Hex(content)},
		},
	}

	completed, err := scanCompletedFiles(dir, manifest)
	require.NoError(t, err)
	require.Empty(t, completed)
}

func TestScanCompletedFiles_HashMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("hello world")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), content, 0644))

	manifest := &snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.txt", Size: uint64(len(content)), Sha256: "0000000000000000000000000000000000000000000000000000000000000000"},
		},
	}

	completed, err := scanCompletedFiles(dir, manifest)
	require.NoError(t, err)
	require.Empty(t, completed)
}

func TestScanCompletedFiles_NilManifest(t *testing.T) {
	t.Parallel()

	completed, err := scanCompletedFiles(t.TempDir(), nil)
	require.NoError(t, err)
	require.Nil(t, completed)
}

func TestScanCompletedFiles_IgnoresTmpFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	content := []byte("partial")
	// Only a .tmp file exists — not a completed file.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt.tmp"), content, 0644))

	manifest := &snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.txt", Size: uint64(len(content)), Sha256: sha256Hex(content)},
		},
	}

	completed, err := scanCompletedFiles(dir, manifest)
	require.NoError(t, err)
	require.Empty(t, completed)
}

func TestManifestTotalSize(t *testing.T) {
	t.Parallel()

	manifest := &snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.txt", Size: 100},
			{Path: "b.txt", Size: 200},
			{Path: "c.txt", Size: 300},
		},
	}

	require.Equal(t, uint64(600), manifestTotalSize(manifest))
}
