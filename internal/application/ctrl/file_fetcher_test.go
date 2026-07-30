package ctrl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

func TestFileFetcher_FetchFileOnceRejectsNonLocalPath(t *testing.T) {
	t.Parallel()

	client, csState := newMockSnapshotClient(t, nil, nil, nil)
	fetcher := &fileFetcher{client: client, sessionID: "test-session"}

	err := fetcher.fetchFileOnce(t.Context(), &snapshotpb.FileEntry{Path: "../outside"}, t.TempDir(), nil)
	require.ErrorContains(t, err, "invalid snapshot path")
	require.Equal(t, int32(0), csState.fetchFileCalls.Load())
}

func TestFileFetcher_FetchFileOnceRejectsMissingTargetRoot(t *testing.T) {
	t.Parallel()

	client, csState := newMockSnapshotClient(t, nil, nil, nil)
	fetcher := &fileFetcher{client: client, sessionID: "test-session"}
	missingRoot := filepath.Join(t.TempDir(), "missing")

	err := fetcher.fetchFileOnce(t.Context(), &snapshotpb.FileEntry{Path: "data.txt"}, missingRoot, nil)
	require.ErrorContains(t, err, "opening snapshot target directory")
	require.Equal(t, int32(0), csState.fetchFileCalls.Load())
}

func TestFileFetcher_FetchFileOnceReportsTempCreateFailure(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(targetDir, "data.txt.tmp"), 0755))

	content := []byte("data")
	client, _ := newMockSnapshotClient(t, nil, nil, map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.txt": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{{Data: content, Eof: true}},
			failAt:    -1,
		}),
	})
	fetcher := &fileFetcher{client: client, sessionID: "test-session"}

	err := fetcher.fetchFileOnce(t.Context(), &snapshotpb.FileEntry{
		Path:   "data.txt",
		Sha256: fileSHA256(content),
	}, targetDir, nil)
	require.ErrorContains(t, err, "creating temp file")
}

func TestFileFetcher_FetchFileOnceRemovesTempAfterRenameFailure(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(targetDir, "data.txt"), 0755))

	content := []byte("data")
	client, _ := newMockSnapshotClient(t, nil, nil, map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.txt": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{{Data: content, Eof: true}},
			failAt:    -1,
		}),
	})
	fetcher := &fileFetcher{client: client, sessionID: "test-session"}

	err := fetcher.fetchFileOnce(t.Context(), &snapshotpb.FileEntry{
		Path:   "data.txt",
		Sha256: fileSHA256(content),
	}, targetDir, nil)
	require.ErrorContains(t, err, "renaming data.txt")
	require.NoFileExists(t, filepath.Join(targetDir, "data.txt.tmp"))
}
