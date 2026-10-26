package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
)

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source ../../../internal/infra/backup/storage.go -destination bootstrap_generated_test.go -package store
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -build_constraint=s3 -source ../../../internal/application/admission/admission.go -destination bootstrap_retry_proposer_generated_test.go -package store
//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -build_constraint=s3 -source ../../../internal/infra/state/machine.go -destination bootstrap_retry_notifier_generated_test.go -package store

func bootstrapCheckpointFile(data string) backup.CheckpointFile {
	return backup.CheckpointFile{
		Key:  fmt.Sprintf("bucket/data/checkpoint.%x", sha256.Sum256([]byte(data))),
		Size: int64(len(data)),
	}
}

func TestDownloadBootstrapCheckpointRejectsTraversal(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.MkdirAll(outside, 0o755))
	sentinel := filepath.Join(outside, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("untouched"), 0o600))
	data := "checkpoint bytes"
	cf := bootstrapCheckpointFile(data)
	storage := NewMockStorage(gomock.NewController(t))
	// Serve valid bytes even on the vulnerable implementation, so content
	// verification cannot mask the escaped overwrite.
	storage.EXPECT().GetFile(gomock.Any(), cf.Key).
		Return(io.NopCloser(strings.NewReader(data)), nil).AnyTimes()

	_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{
		"../../outside/sentinel": cf,
	})
	bytes, readErr := os.ReadFile(sentinel)
	require.NoError(t, readErr)
	require.Equal(t, "untouched", string(bytes))
	require.ErrorContains(t, err, "non-local checkpoint filename")
}

func TestDownloadBootstrapCheckpointRejectsSymlinkEscape(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"parent", "leaf", "staging outside", "staging inside", "staging file"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			parent := t.TempDir()
			dataDir := filepath.Join(parent, "data")
			staging := filepath.Join(dataDir, "restore-staging")
			outside := filepath.Join(parent, "outside")
			if kind == "staging inside" {
				outside = filepath.Join(dataDir, "other")
			}
			require.NoError(t, os.MkdirAll(dataDir, 0o755))
			require.NoError(t, os.MkdirAll(outside, 0o755))
			sentinel := filepath.Join(outside, "sentinel")
			require.NoError(t, os.WriteFile(sentinel, []byte("untouched"), 0o600))
			filename := "sentinel"
			switch kind {
			case "parent", "leaf":
				require.NoError(t, os.MkdirAll(staging, 0o755))
				if kind == "parent" {
					require.NoError(t, os.Symlink(outside, filepath.Join(staging, "link")))
					// Parent creation must not create new/ outside staging.
					filename = "link/new/file"
				} else {
					require.NoError(t, os.Symlink(sentinel, filepath.Join(staging, filename)))
				}
			case "staging file":
				require.NoError(t, os.WriteFile(staging, []byte("not a directory"), 0o600))
			default:
				require.NoError(t, os.Symlink(outside, staging))
			}
			cf := bootstrapCheckpointFile("checkpoint bytes")
			storage := NewMockStorage(gomock.NewController(t))
			if !strings.HasPrefix(kind, "staging") {
				storage.EXPECT().GetFile(gomock.Any(), cf.Key).
					Return(io.NopCloser(strings.NewReader("checkpoint bytes")), nil).AnyTimes()
			}

			_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{filename: cf})
			bytes, readErr := os.ReadFile(sentinel)
			require.NoError(t, readErr)
			require.Equal(t, "untouched", string(bytes))
			entries, readErr := os.ReadDir(outside)
			require.NoError(t, readErr)
			require.Len(t, entries, 1, "no parent directories may be created outside staging")
			if strings.HasPrefix(kind, "staging") {
				require.ErrorContains(t, err, "staging must be a real directory")
			} else {
				// Previous-attempt children are discarded without following their
				// symlinks; the checkpoint is written into a fresh directory.
				require.NoError(t, err)
				data, readErr := os.ReadFile(filepath.Join(staging, filename))
				require.NoError(t, readErr)
				require.Equal(t, "checkpoint bytes", string(data))
			}
		})
	}
}

func TestDownloadBootstrapCheckpointResetFailure(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("requires POSIX directory permissions enforced for the current user")
	}

	for _, previous := range []bool{false, true} {
		t.Run(fmt.Sprintf("previous-staging=%t", previous), func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			locked := dataDir
			wantError := "creating fresh staging directory"
			if previous {
				locked = filepath.Join(dataDir, "restore-staging")
				require.NoError(t, os.Mkdir(locked, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(locked, "old.log"), []byte("old"), 0o600))
				wantError = "discarding previous staging directory"
			}
			require.NoError(t, os.Chmod(locked, 0o500))
			t.Cleanup(func() { require.NoError(t, os.Chmod(locked, 0o755)) })
			storage := NewMockStorage(gomock.NewController(t))
			_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{
				"CURRENT": bootstrapCheckpointFile("checkpoint bytes"),
			})
			require.ErrorContains(t, err, wantError)
			require.ErrorIs(t, err, os.ErrPermission)
			require.NoFileExists(t, filepath.Join(dataDir, "RESTORED"))
			require.NoDirExists(t, filepath.Join(dataDir, "checkpoints"))
		})
	}
}

type bootstrapCheckpointReader struct {
	io.Reader

	close func() error
}

func (r bootstrapCheckpointReader) Close() error { return r.close() }

func TestDownloadBootstrapCheckpointRejectsParentSymlinkBetweenFiles(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	staging := filepath.Join(dataDir, "restore-staging")
	outside := filepath.Join(parent, "outside")
	require.NoError(t, os.Mkdir(outside, 0o755))
	storage := NewMockStorage(gomock.NewController(t))
	cf := bootstrapCheckpointFile("checkpoint bytes")
	// Both entries share a parent, so map iteration order cannot affect the
	// trigger. After the first copy, replace that parent before the next MkdirAll.
	storage.EXPECT().GetFile(gomock.Any(), cf.Key).Return(bootstrapCheckpointReader{
		Reader: strings.NewReader("checkpoint bytes"),
		close: func() error {
			if err := os.Rename(filepath.Join(staging, "link"), filepath.Join(staging, "downloaded")); err != nil {
				t.Fatal(err)
			}
			require.NoError(t, os.Symlink(outside, filepath.Join(staging, "link")))

			return nil
		},
	}, nil)
	_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{
		"link/new/a": cf,
		"link/new/b": cf,
	})
	require.ErrorContains(t, err, "creating directory for link/new/")
	entries, readErr := os.ReadDir(outside)
	require.NoError(t, readErr)
	require.Empty(t, entries, "parent creation must remain confined after staging reset")
}

func TestDownloadBootstrapCheckpointValidFiles(t *testing.T) {
	t.Parallel()

	for _, filename := range []string{"CURRENT", "nested/checkpoint"} {
		t.Run(filename, func(t *testing.T) {
			t.Parallel()

			dataDir := filepath.Join(t.TempDir(), "data")
			data := "checkpoint bytes"
			cf := bootstrapCheckpointFile(data)
			storage := NewMockStorage(gomock.NewController(t))
			storage.EXPECT().GetFile(gomock.Any(), cf.Key).Return(io.NopCloser(strings.NewReader(data)), nil)
			n, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{filename: cf})
			require.NoError(t, err)
			require.EqualValues(t, 16, n)
			bytes, err := os.ReadFile(filepath.Join(dataDir, "restore-staging", filename))
			require.NoError(t, err)
			require.Equal(t, "checkpoint bytes", string(bytes))
			require.NoFileExists(t, filepath.Join(dataDir, "RESTORED"))
		})
	}
}

func TestDownloadBootstrapCheckpointEmptyCreatesStaging(t *testing.T) {
	t.Parallel()

	dataDir := filepath.Join(t.TempDir(), "data")
	storage := NewMockStorage(gomock.NewController(t))
	n, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, nil)
	require.NoError(t, err)
	require.Zero(t, n)
	require.DirExists(t, filepath.Join(dataDir, "restore-staging"))
}

func TestDownloadBootstrapCheckpointDiscardsPreviousAttempt(t *testing.T) {
	t.Parallel()

	for _, exportOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("export-only=%t", exportOnly), func(t *testing.T) {
			t.Parallel()

			dataDir := t.TempDir()
			staging := filepath.Join(dataDir, "restore-staging")
			sibling := filepath.Join(dataDir, "operator-notes")
			require.NoError(t, os.WriteFile(sibling, []byte("untouched"), 0o600))
			require.NoError(t, os.MkdirAll(filepath.Join(staging, "nested"), 0o755))
			for _, name := range []string{"000099.log", "nested/old-projection"} {
				require.NoError(t, os.WriteFile(filepath.Join(staging, name), []byte("previous attempt"), 0o600))
			}
			storage := NewMockStorage(gomock.NewController(t))
			files := map[string]backup.CheckpointFile{}
			if !exportOnly {
				cf := bootstrapCheckpointFile("checkpoint bytes")
				files["CURRENT"] = cf
				storage.EXPECT().GetFile(gomock.Any(), cf.Key).Return(io.NopCloser(strings.NewReader("checkpoint bytes")), nil)
			}

			_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, files)
			require.NoError(t, err)
			require.NoFileExists(t, filepath.Join(staging, "000099.log"))
			require.NoDirExists(t, filepath.Join(staging, "nested"))
			notes, err := os.ReadFile(sibling)
			require.NoError(t, err)
			require.Equal(t, "untouched", string(notes))
			entries, err := os.ReadDir(staging)
			require.NoError(t, err)
			require.Len(t, entries, len(files))
			if !exportOnly {
				data, err := os.ReadFile(filepath.Join(staging, "CURRENT"))
				require.NoError(t, err)
				require.Equal(t, "checkpoint bytes", string(data))
			}
		})
	}
}

func TestDownloadBootstrapCheckpointRejectsNonLocalBeforeStorageRead(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"empty", "parent", "traversal", "absolute"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			parent := t.TempDir()
			dataDir := filepath.Join(parent, "data")
			filename := ""
			switch kind {
			case "parent":
				filename = ".."
			case "traversal":
				filename = "../../outside/new/file"
			case "absolute":
				filename = filepath.Join(parent, "outside", "new", "file")
			}
			// With no GetFile expectation, any remote read fails this test.
			storage := NewMockStorage(gomock.NewController(t))
			_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{
				filename: bootstrapCheckpointFile("checkpoint bytes"),
			})
			require.ErrorContains(t, err, "non-local checkpoint filename")
			entries, err := os.ReadDir(filepath.Join(dataDir, "restore-staging"))
			require.NoError(t, err)
			require.Empty(t, entries)
			require.NoDirExists(t, filepath.Join(parent, "outside"))
		})
	}
}

func TestDownloadBootstrapCheckpointRejectsSymlinkIntroducedDuringDownload(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataDir := filepath.Join(parent, "data")
	sentinel := filepath.Join(parent, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("untouched"), 0o600))
	storage := NewMockStorage(gomock.NewController(t))
	cf := bootstrapCheckpointFile("checkpoint bytes")
	// GetFile runs after parent creation. Insert the symlink at that boundary
	// to prove that the final Create remains confined after lexical validation.
	storage.EXPECT().GetFile(gomock.Any(), cf.Key).DoAndReturn(func(context.Context, string) (io.ReadCloser, error) {
		require.NoError(t, os.Symlink(sentinel, filepath.Join(dataDir, "restore-staging", "file")))

		return io.NopCloser(strings.NewReader("checkpoint bytes")), nil
	})
	_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{"file": cf})
	require.ErrorContains(t, err, "creating file")
	bytes, readErr := os.ReadFile(sentinel)
	require.NoError(t, readErr)
	require.Equal(t, "untouched", string(bytes))
}

func TestDownloadBootstrapCheckpointDownloadFailures(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"missing", "size", "sha256"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			dataDir := t.TempDir()
			storage := NewMockStorage(gomock.NewController(t))
			cf := bootstrapCheckpointFile("checkpoint bytes")
			data := "checkpoint bytes"
			switch kind {
			case "missing":
				storage.EXPECT().GetFile(gomock.Any(), cf.Key).Return(nil, backup.ErrFileNotFound)
			case "size":
				data = "short"
			case "sha256":
				data = "CHECKPOINT BYTES"
			}
			if kind != "missing" {
				storage.EXPECT().GetFile(gomock.Any(), cf.Key).Return(io.NopCloser(strings.NewReader(data)), nil)
			}
			_, err := downloadBootstrapCheckpoint(t.Context(), storage, dataDir, map[string]backup.CheckpointFile{"file": cf})
			if kind == "missing" {
				require.ErrorIs(t, err, backup.ErrFileNotFound)
				require.NoFileExists(t, filepath.Join(dataDir, "restore-staging", "file"))
			} else {
				require.ErrorIs(t, err, backup.ErrCheckpointFileCorrupted)
				require.ErrorContains(t, err, kind+" mismatch")
			}
			require.NoFileExists(t, filepath.Join(dataDir, "RESTORED"))
			require.NoDirExists(t, filepath.Join(dataDir, "checkpoints"))
		})
	}
}
