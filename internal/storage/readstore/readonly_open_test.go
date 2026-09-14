package readstore

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func TestOpenReadOnlyPreservesOpenDirError(t *testing.T) {
	t.Parallel()
	for _, cause := range []error{fs.ErrNotExist, syscall.EIO} {
		t.Run(cause.Error(), func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			path := filepath.Join(t.TempDir(), "checkpoint")
			require.NoError(t, store.CreateCheckpoint(path))
			filesystem := &failingOpenDirFS{FS: vfs.Default, cause: cause, remove: func() { require.NoError(t, os.RemoveAll(path)) }}
			opened, err := openReadOnlyWithFS(path, store.logger, filesystem)
			require.Nil(t, opened)
			require.ErrorIs(t, err, cause)
			if errors.Is(cause, syscall.EIO) {
				require.NotErrorIs(t, err, fs.ErrNotExist)
			}
		})
	}
}

type failingOpenDirFS struct {
	vfs.FS

	cause  error
	remove func()
}

func (f *failingOpenDirFS) OpenDir(name string) (vfs.File, error) {
	f.remove()

	return nil, &os.PathError{Op: "open", Path: name, Err: f.cause}
}
