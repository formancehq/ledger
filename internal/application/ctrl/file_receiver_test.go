package ctrl

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

func TestValidateSnapshotManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "root file", path: "MANIFEST-000001"},
		{name: "nested file", path: filepath.Join("nested", "000001.sst")},
		{name: "empty", path: "", wantErr: true},
		{name: "parent traversal", path: filepath.Join("..", "outside"), wantErr: true},
		{name: "absolute", path: filepath.Join(string(filepath.Separator), "outside"), wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := validateSnapshotManifest(&snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{{Path: test.path}},
			})
			if test.wantErr {
				require.ErrorContains(t, err, "invalid snapshot path")

				return
			}

			require.NoError(t, err)
		})
	}

	require.NoError(t, validateSnapshotManifest(nil))
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
