package readstore

import (
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// readOnlyOpeningFS preserves the filesystem cause that Pebble discards when
// opening a missing read-only database directory. Record the failed operation,
// rather than inspecting a path whose state may change after the failure.
// Pebble retains the FS after a successful open, so access stays synchronized.
type readOnlyOpeningFS struct {
	vfs.FS

	mu  sync.Mutex
	err error
}

func (f *readOnlyOpeningFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		f.mu.Lock()
		if f.err == nil {
			f.err = err
		}
		f.mu.Unlock()
	}

	return file, err
}

func (f *readOnlyOpeningFS) openDirError() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.err
}
