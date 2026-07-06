package store

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnsureDisjointDirs exercises the guards that prevent `--usage-dir`
// from silently wiping the primary Pebble store. The command RemoveAlls the
// usage dir before opening data, so any overlap is destructive.
func TestEnsureDisjointDirs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	liveDir := filepath.Join(dataDir, "live")

	tests := []struct {
		name    string
		usage   string
		wantErr string
	}{
		{
			name:  "sibling under data-dir (default)",
			usage: filepath.Join(dataDir, "usage"),
		},
		{
			name:  "fully separate root",
			usage: filepath.Join(root, "elsewhere", "usage"),
		},
		{
			name:    "equal to data-dir",
			usage:   dataDir,
			wantErr: "must not equal --data-dir",
		},
		{
			name:    "parent of data-dir",
			usage:   root,
			wantErr: "must not be a parent of --data-dir",
		},
		{
			name:    "equal to <data-dir>/live",
			usage:   liveDir,
			wantErr: "must not equal the primary Pebble live directory",
		},
		{
			name:    "parent of <data-dir>/live but distinct from data-dir",
			usage:   filepath.Join(dataDir),
			wantErr: "must not equal --data-dir",
		},
		{
			name:    "inside <data-dir>/live",
			usage:   filepath.Join(liveDir, "nested"),
			wantErr: "must not live inside the primary Pebble directory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ensureDisjointDirs(dataDir, tc.usage)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}
