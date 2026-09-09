package readstore_test

import (
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
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

func TestLedgerHistoryStateSurvivesCheckpoint(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	wb := readstore.NewWriteBatch()
	batch := store.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerHistoryState(dal.NewKeyBuilder(), "ledger", 2))
	require.NoError(t, wb.Flush())

	checkpointDir := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, store.CreateCheckpoint(checkpointDir))

	frozen, err := readstore.OpenReadOnly(checkpointDir, discardLogger{})
	require.NoError(t, err)
	defer func() { require.NoError(t, frozen.Close()) }()

	snapshot := frozen.NewSnapshot()
	entries, err := readstore.ReadAllLedgerHistoryStatesFrom(snapshot)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())
	require.Equal(t, []readstore.LedgerHistoryStateEntry{{LedgerName: "ledger", State: 2}}, entries)
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

type failingLedgerHistoryReader struct {
	err error
}

func (r failingLedgerHistoryReader) Get([]byte) ([]byte, io.Closer, error) {
	return nil, nil, r.err
}

func (r failingLedgerHistoryReader) NewIter(*pebble.IterOptions) (*pebble.Iterator, error) {
	return nil, r.err
}

func TestLedgerHistoryStatePropagatesIteratorCreationError(t *testing.T) {
	t.Parallel()

	want := errors.New("iterator unavailable")
	_, err := readstore.ReadAllLedgerHistoryStatesFrom(failingLedgerHistoryReader{err: want})
	require.ErrorIs(t, err, want)
}

func TestLedgerHistoryStateRejectsMalformedKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix []byte
		want   string
	}{
		{name: "short suffix", suffix: []byte("short"), want: "got 5-byte suffix"},
		{name: "empty ledger", suffix: make([]byte, dal.LedgerNameFixedSize), want: "empty ledger name"},
		{name: "embedded NUL", suffix: func() []byte {
			suffix := make([]byte, dal.LedgerNameFixedSize)
			copy(suffix, []byte("bad\x00name"))

			return suffix
		}(), want: "embedded NUL byte"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
			require.NoError(t, err)
			defer func() { _ = store.Close() }()

			key := append(append([]byte{}, readstore.LedgerHistoryStatePrefix()...), test.suffix...)
			batch := store.NewBatch()
			require.NoError(t, batch.SetBytes(key, []byte{1}))
			require.NoError(t, batch.Commit())

			snapshot := store.NewSnapshot()
			_, err = readstore.ReadAllLedgerHistoryStatesFrom(snapshot)
			require.ErrorIs(t, err, readstore.ErrLedgerHistoryCorrupt)
			require.ErrorContains(t, err, test.want)
			require.NoError(t, snapshot.Close())
		})
	}
}
