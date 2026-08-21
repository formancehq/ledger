package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

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
