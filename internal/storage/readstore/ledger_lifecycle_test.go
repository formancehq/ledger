package readstore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestLedgerLifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	rs, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })
	kb := dal.NewKeyBuilder()
	wb := readstore.NewWriteBatch()

	batch := rs.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerLifecycle(kb, "ledger", 42, true))
	require.NoError(t, batch.Commit())

	snap := rs.NewSnapshot()
	lifecycle, ok, err := readstore.ReadLedgerLifecycle(snap, kb, "ledger")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, readstore.LedgerLifecycle{ID: 42, Active: true}, lifecycle)
	require.NoError(t, snap.Close())

	batch = rs.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerLifecycle(kb, "ledger", 0, false))
	require.NoError(t, batch.Commit())

	snap = rs.NewSnapshot()
	lifecycle, ok, err = readstore.ReadLedgerLifecycle(snap, kb, "ledger")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, readstore.LedgerLifecycle{Active: false}, lifecycle)
	require.NoError(t, snap.Close())
}
