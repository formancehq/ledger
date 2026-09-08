package readstore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestLedgerHistoryStateRoundTripAndDelete(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	wb := readstore.NewWriteBatch()
	batch := store.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerHistoryState(dal.NewKeyBuilder(), "empty", 1))
	require.NoError(t, wb.WriteLedgerHistoryState(dal.NewKeyBuilder(), "non-empty", 2))
	require.False(t, wb.Empty(), "tracker-only batches must be committed")
	require.NoError(t, wb.Flush())

	snapshot := store.NewSnapshot()
	entries, err := readstore.ReadAllLedgerHistoryStatesFrom(snapshot)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.Len(t, entries, 2)
	got := map[string]byte{}
	for _, entry := range entries {
		got[entry.LedgerName] = entry.State
	}
	assert.Equal(t, map[string]byte{"empty": 1, "non-empty": 2}, got)

	deleteBatch := store.NewBatch()
	wb.Init(deleteBatch)
	require.NoError(t, wb.DeleteLedgerHistoryState(dal.NewKeyBuilder(), "empty"))
	require.False(t, wb.Empty())
	require.NoError(t, wb.Flush())

	snapshot = store.NewSnapshot()
	entries, err = readstore.ReadAllLedgerHistoryStatesFrom(snapshot)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.Equal(t, []readstore.LedgerHistoryStateEntry{{LedgerName: "non-empty", State: 2}}, entries)
}

func TestLedgerHistoryStateRejectsMalformedValue(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	batch := store.NewBatch()
	require.NoError(t, batch.SetBytes(readstore.LedgerHistoryStateKey(dal.NewKeyBuilder(), "broken"), []byte{1, 2}))
	require.NoError(t, batch.Commit())

	snapshot := store.NewSnapshot()
	_, err = readstore.ReadAllLedgerHistoryStatesFrom(snapshot)
	require.ErrorContains(t, err, "got 2 bytes, want 1")
	require.NoError(t, snapshot.Close())
}
