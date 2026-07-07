package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// The reverted_at index keys the ORIGINAL transaction (the one being reverted)
// by the time it was reverted — the compensating transaction's timestamp — so a
// reverted_at range query returns the originals. All values come from the
// RevertedTransaction log, so backfill via log replay covers historical reverts.
func TestIndexRevertedTransaction_WritesRevertedAtIndex(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.initBatch(batch)

	cfg := newLedgerIndexConfig()
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	const (
		originalID uint64 = 7
		revertID   uint64 = 8
		revertTs   uint64 = 1_700_000_000_000_000
	)

	rt := &commonpb.RevertedTransaction{
		RevertedTransactionId: originalID,
		RevertTransaction: &commonpb.Transaction{
			Id:        revertID,
			Timestamp: &commonpb.Timestamp{Data: revertTs},
		},
	}

	require.NoError(t, b.indexRevertedTransaction(b.kb, cfg, "test", rt, nil))
	require.NoError(t, b.wb.Flush())

	// Entry keyed to the original transaction by the revert timestamp.
	require.True(t, readStoreKeyExists(t, store,
		readstore.TransactionRevertedAtKey(dal.NewKeyBuilder(), "test", revertTs, originalID)))

	// Never keyed to the compensating transaction.
	require.False(t, readStoreKeyExists(t, store,
		readstore.TransactionRevertedAtKey(dal.NewKeyBuilder(), "test", revertTs, revertID)))
}
