package ctrl

import "github.com/formancehq/ledger/v3/internal/proto/snapshotpb"

// manifestTotalSize returns the sum of all file sizes in the manifest.
func manifestTotalSize(manifest *snapshotpb.SnapshotManifest) uint64 {
	var total uint64
	for _, e := range manifest.GetFiles() {
		total += e.GetSize()
	}

	return total
}
