package ledgerstore_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestGetLastLog(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := core.Now()

	lastLog, err := store.GetLastLog(context.Background())
	require.True(t, storage.IsNotFoundError(err))
	require.Nil(t, lastLog)
	tx1 := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "tx1",
				Timestamp: now.Add(-3 * time.Hour),
			},
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
	}

	logTx := core.NewTransactionLog(&tx1.Transaction, nil).ChainLog(nil)
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
	persistedLog := appendLog(t, store, logTx.ChainLog(nil))

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
	).ChainLog(nil))

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
	chainedLog := appendLog(t, store, logTx.ChainLog(nil))

	logFromDB, err := store.ReadLogForCreatedTransaction(context.Background(), 0)
	require.NoError(t, err)
	require.NotNil(t, logFromDB)
	require.Equal(t, *chainedLog, *logFromDB)
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

	ret := appendLog(t, store, log.ChainLog(nil))

	lastLog, err := store.ReadLogWithIdempotencyKey(context.Background(), "test")
	require.NoError(t, err)
	require.NotNil(t, lastLog)
	require.Equal(t, *ret, *lastLog)
}

func TestGetLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := core.Now()

	tx1 := core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "tx1",
				Timestamp: now.Add(-3 * time.Hour),
			},
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
	}
	tx2 := core.ExpandedTransaction{
		Transaction: core.Transaction{
			ID: 1,
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "tx2",
				Timestamp: now.Add(-2 * time.Hour),
			},
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(200),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
	}
	tx3 := core.ExpandedTransaction{
		Transaction: core.Transaction{
			ID: 2,
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "central_bank",
						Destination: "users:1",
						Amount:      big.NewInt(1),
						Asset:       "USD",
					},
				},
				Reference: "tx3",
				Metadata: metadata.Metadata{
					"priority": "high",
				},
				Timestamp: now.Add(-1 * time.Hour),
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"central_bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
			"users:1": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"central_bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(1),
				},
			},
			"users:1": {
				"USD": {
					Input:  big.NewInt(1),
					Output: big.NewInt(0),
				},
			},
		},
	}

	var previousLog *core.ChainedLog
	for _, tx := range []core.ExpandedTransaction{tx1, tx2, tx3} {
		newLog := core.NewTransactionLog(&tx.Transaction, nil).ChainLog(previousLog)
		appendLog(t, store, newLog)
		previousLog = newLog
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

func TestGetBalanceFromLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	const (
		batchNumber = 100
		batchSize   = 10
		input       = 100
		output      = 10
	)

	logs := make([]*core.ActiveLog, 0)
	var previousLog *core.ChainedLog
	for i := 0; i < batchNumber; i++ {
		for j := 0; j < batchSize; j++ {
			activeLog := core.NewActiveLog(core.NewTransactionLog(
				core.NewTransaction().WithPostings(
					core.NewPosting("world", fmt.Sprintf("account:%d", j), "EUR/2", big.NewInt(input)),
					core.NewPosting(fmt.Sprintf("account:%d", j), "starbucks", "EUR/2", big.NewInt(output)),
				).WithID(uint64(i*batchSize+j)),
				map[string]metadata.Metadata{},
			).ChainLog(previousLog))
			logs = append(logs, activeLog)
			previousLog = activeLog.ChainedLog
		}
	}
	err := store.InsertLogs(context.Background(), logs...)
	require.NoError(t, err)

	balance, err := store.GetBalanceFromLogs(context.Background(), "account:1", "EUR/2")
	require.NoError(t, err)
	require.Equal(t, big.NewInt((input-output)*batchNumber), balance)
}

func TestGetMetadataFromLogs(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logs := make([]*core.ActiveLog, 0)
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		),
		map[string]metadata.Metadata{},
	).ChainLog(nil)))
	logs = append(logs, core.NewActiveLog(core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: metadata.Metadata{
			"foo": "bar",
		},
	}).ChainLog(logs[0].ChainedLog)))
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		).WithID(1),
		map[string]metadata.Metadata{},
	).ChainLog(logs[1].ChainedLog)))
	logs = append(logs, core.NewActiveLog(core.NewSetMetadataLog(core.Now(), core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: metadata.Metadata{
			"role": "admin",
		},
	}).ChainLog(logs[2].ChainedLog)))
	logs = append(logs, core.NewActiveLog(core.NewTransactionLog(
		core.NewTransaction().WithPostings(
			core.NewPosting("world", "bank", "EUR/2", big.NewInt(100)),
			core.NewPosting("bank", "starbucks", "EUR/2", big.NewInt(10)),
		).WithID(2),
		map[string]metadata.Metadata{},
	).ChainLog(logs[3].ChainedLog)))

	err := store.InsertLogs(context.Background(), logs...)
	require.NoError(t, err)

	metadata, err := store.GetMetadataFromLogs(context.Background(), "bank", "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", metadata)
}
