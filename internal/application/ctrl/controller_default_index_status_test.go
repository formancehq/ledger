package ctrl

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// IndexFileSize must report the read index's on-disk footprint. A stat of the
// read index directory returns the directory entry size (4096 on ext4), which
// stays constant however much the index holds.
func TestGetIndexStatusReportsReadIndexDiskUsage(t *testing.T) {
	t.Parallel()

	ctrl, rs := newAuditAlignmentController(t, 0)

	before, err := ctrl.GetIndexStatus(context.Background(), &servicepb.GetIndexStatusRequest{})
	require.NoError(t, err)

	batch := rs.NewBatch()
	kb := dal.NewKeyBuilder()
	for sequence := range uint64(2000) {
		ledger := "ledger-" + strconv.FormatUint(sequence, 10)
		require.NoError(t, batch.SetBytes(readstore.AuditIndexStringKey(kb, readstore.AuditFieldLedger, ledger, sequence), nil))
	}
	require.NoError(t, batch.Commit())
	// The read index runs without a WAL: rows reach disk only on flush.
	require.NoError(t, rs.DB().Flush())

	after, err := ctrl.GetIndexStatus(context.Background(), &servicepb.GetIndexStatusRequest{})
	require.NoError(t, err)

	require.Equal(t, rs.DiskSpaceUsage(), after.GetIndexFileSize(),
		"IndexFileSize must come from the read index's Pebble disk usage")
	require.Greater(t, after.GetIndexFileSize(), before.GetIndexFileSize(),
		"IndexFileSize must grow once index rows are flushed to disk")
}
