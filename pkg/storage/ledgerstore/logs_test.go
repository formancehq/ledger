package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestGetLastLog(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	lastLog, err := store.GetLastLog(context.Background())
	require.True(t, errors.IsNotFoundError(err))
	require.Nil(t, lastLog)

	logTx := core.NewTransactionLog(&tx1.Transaction, nil)
	appendLog(t, store, logTx)

	lastLog, err = store.GetLastLog(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lastLog)

	require.Equal(t, tx1.Postings, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx1.Reference, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx1.Timestamp, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Timestamp)
}

func TestReadLogForCreatedTransactionWithReference(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logTx := core.NewTransactionLog(
		core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "bank", "USD", big.NewInt(100)),
			).
			WithReference("ref"),
		map[string]metadata.Metadata{},
	)
	persistedLog := appendLog(t, store, logTx)

	lastLog, err := store.ReadLogForCreatedTransactionWithReference(context.Background(), "ref")
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, *persistedLog, *lastLog)
}

func TestReadLogForRevertedTransaction(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	persistedLog := appendLog(t, store, core.NewRevertedTransactionLog(
		core.Now(),
		0,
		core.NewTransaction(),
	))

	lastLog, err := store.ReadLogForRevertedTransaction(context.Background(), 0)
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, *persistedLog, *lastLog)
}

func TestReadLogForCreatedTransaction(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logTx := core.NewTransactionLog(
		core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "bank", "USD", big.NewInt(100)),
			).
			WithReference("ref"),
		map[string]metadata.Metadata{},
	)
	persistedLog := appendLog(t, store, logTx)

	lastLog, err := store.ReadLogForCreatedTransaction(context.Background(), 0)
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, *persistedLog, *lastLog)
}

func TestReadLogWithIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logTx := core.NewTransactionLog(
		core.NewTransaction().
			WithPostings(
				core.NewPosting("world", "bank", "USD", big.NewInt(100)),
			),
		map[string]metadata.Metadata{},
	)
	log := logTx.WithIdempotencyKey("test")

	ret := appendLog(t, store, log)

	lastLog, err := store.ReadLogWithIdempotencyKey(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, *ret, *lastLog)
}

func TestGetLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	for _, tx := range []core.ExpandedTransaction{tx1, tx2, tx3} {
		appendLog(t, store, core.NewTransactionLog(&tx.Transaction, nil))
	}

	cursor, err := store.GetLogs(context.Background(), ledgerstore.NewLogsQuery())
	require.NoError(t, err)
	require.Equal(t, ledgerstore.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.Equal(t, uint64(2), cursor.Data[0].ID)
	require.Equal(t, tx3.Postings, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx3.Reference, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx3.Timestamp, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Timestamp)

	cursor, err = store.GetLogs(context.Background(), ledgerstore.NewLogsQuery().WithPageSize(1))
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.Equal(t, uint64(2), cursor.Data[0].ID)

	cursor, err = store.GetLogs(context.Background(), ledgerstore.NewLogsQuery().
		WithStartTimeFilter(now.Add(-2*time.Hour)).
		WithEndTimeFilter(now.Add(-1*time.Hour)).
		WithPageSize(10))
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.Equal(t, uint64(1), cursor.Data[0].ID)
}
