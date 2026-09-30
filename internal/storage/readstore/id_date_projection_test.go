package readstore_test

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestIDDateRangeIterator_MissingCompanionFails(t *testing.T) {
	t.Parallel()
	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	kb := dal.NewKeyBuilder()
	require.NoError(t, store.DB().Set(readstore.TransactionTimestampKey(kb, "ledger", 100, 3), nil, pebble.NoSync))
	prefix := readstore.TransactionTimestampRangePrefix(kb, "ledger")
	lower := append(append([]byte(nil), prefix...), readstore.EncodeTxID(nil, 100)...)
	upper := append(append([]byte(nil), prefix...), readstore.EncodeTxID(nil, 101)...)
	idPrefix := readstore.IDDatePrefix(kb, readstore.PrefixTransactionTimestampByID, "ledger")
	it, err := readstore.NewIDDateRangeIterator[readstore.Asc](store.DB(), idPrefix, lower, upper, len(prefix)+8, 100, 101, true, true, false, 0)
	require.NoError(t, err)
	defer it.Close()
	require.False(t, it.Next())
	require.ErrorContains(t, it.Err(), "no ID-first companion")
}

func TestBuiltinDates_WriteBothViewsAndDeleteLedger(t *testing.T) {
	t.Parallel()
	store, err := readstore.New(t.TempDir(), discardLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	wb := readstore.NewWriteBatch()
	wb.Init(batch)
	wb.SetEventSequence(7)
	require.NoError(t, wb.WriteTransactionTimestampIndex(kb, "drop", 100, 3))
	require.NoError(t, wb.WriteTransactionInsertedAtIndex(kb, "drop", 101, 3))
	require.NoError(t, wb.WriteTransactionRevertedAtIndex(kb, "drop", 102, 3))
	require.NoError(t, wb.WriteLedgerLogDateIndex(kb, "drop", 103, 4))
	require.NoError(t, wb.WriteTransactionTimestampIndex(kb, "keep", 200, 5))
	require.NoError(t, batch.Commit())

	for _, tc := range []struct {
		ledger   string
		prefix   byte
		id, date uint64
		stamped  bool
	}{
		{"drop", readstore.PrefixTransactionTimestampByID, 3, 100, false},
		{"drop", readstore.PrefixTransactionInsertedAtByID, 3, 101, false},
		{"drop", readstore.PrefixTransactionRevertedAtByID, 3, 102, true},
		{"drop", readstore.PrefixLedgerLogDateByID, 4, 103, false},
		{"keep", readstore.PrefixTransactionTimestampByID, 5, 200, false},
	} {
		value, closer, gErr := store.DB().Get(readstore.IDDateKey(kb, tc.prefix, tc.ledger, tc.id))
		require.NoError(t, gErr)
		require.Equal(t, tc.date, binary.BigEndian.Uint64(value[:8]))
		if tc.stamped {
			require.Equal(t, uint64(7), binary.BigEndian.Uint64(value[8:]))
		} else {
			require.Len(t, value, 8)
		}
		require.NoError(t, closer.Close())
	}
	drop := store.NewBatch()
	require.NoError(t, readstore.DeleteLedgerIndexes(drop, "drop"))
	require.NoError(t, drop.Commit())
	for _, prefix := range []byte{readstore.PrefixTransactionTimestampByID, readstore.PrefixTransactionInsertedAtByID, readstore.PrefixTransactionRevertedAtByID, readstore.PrefixLedgerLogDateByID} {
		id := uint64(3)
		if prefix == readstore.PrefixLedgerLogDateByID {
			id = 4
		}
		_, closer, gErr := store.DB().Get(readstore.IDDateKey(kb, prefix, "drop", id))
		require.ErrorIs(t, gErr, pebble.ErrNotFound)
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	}
	_, closer, gErr := store.DB().Get(readstore.IDDateKey(kb, readstore.PrefixTransactionTimestampByID, "keep", 5))
	require.NoError(t, gErr)
	require.NoError(t, closer.Close())
}
