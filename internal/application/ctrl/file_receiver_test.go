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

func TestValidateSnapshotManifest_RejectsCollidingEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		paths   []string
		wantErr string
	}{
		{
			name:    "duplicate path",
			paths:   []string{"a.bin", "b.bin", "a.bin"},
			wantErr: "duplicate snapshot path at manifest entries 0 and 2",
		},
		{
			name:    "duplicate path after cleaning",
			paths:   []string{"a.bin", "." + string(filepath.Separator) + "a.bin"},
			wantErr: "duplicate snapshot path at manifest entries 0 and 1",
		},
		{
			name:    "staging collision after cleaning",
			paths:   []string{"a.bin", "." + string(filepath.Separator) + "a.bin.tmp"},
			wantErr: "snapshot path at manifest entry 1 collides with the staging path of entry 0",
		},
		{
			name:    "staging collision under a trailing separator",
			paths:   []string{"a.bin" + string(filepath.Separator), filepath.Join("a.bin", ".tmp")},
			wantErr: "snapshot path at manifest entry 1 collides with the staging path of entry 0",
		},
		{
			name:    "path equals another entry's staging path",
			paths:   []string{"a.bin", "a.bin.tmp"},
			wantErr: "snapshot path at manifest entry 1 collides with the staging path of entry 0",
		},
		{
			name:    "staging collision declared before its final path",
			paths:   []string{"a.bin.tmp", "a.bin"},
			wantErr: "snapshot path at manifest entry 0 collides with the staging path of entry 1",
		},
		{
			name:    "nested staging collision",
			paths:   []string{filepath.Join("nested", "000001.sst"), filepath.Join("nested", "000001.sst.tmp")},
			wantErr: "snapshot path at manifest entry 1 collides with the staging path of entry 0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			files := make([]*snapshotpb.FileEntry, 0, len(test.paths))
			for _, path := range test.paths {
				files = append(files, &snapshotpb.FileEntry{Path: path})
			}

			require.ErrorContains(t, validateSnapshotManifest(&snapshotpb.SnapshotManifest{Files: files}), test.wantErr)
		})
	}
}

func TestValidateSnapshotManifest_AcceptsDistinctStagingNames(t *testing.T) {
	t.Parallel()

	// A ".tmp" suffix is only rejected when it shadows another entry's staging
	// path; on its own it is a legitimate manifest path.
	require.NoError(t, validateSnapshotManifest(&snapshotpb.SnapshotManifest{
		Files: []*snapshotpb.FileEntry{
			{Path: "a.bin.tmp"},
			{Path: "b.bin"},
			{Path: filepath.Join("nested", "c.bin")},
		},
	}))
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
