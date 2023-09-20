package ledgerstore_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/ledger/internal/storage/query"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestGetLastLog(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	lastLog, err := store.GetLastLog(context.Background())
	require.True(t, storage.IsNotFoundError(err))
	require.Nil(t, lastLog)
	tx1 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(0),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
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
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
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
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
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

	logTx := ledger.NewTransactionLog(&tx1.Transaction, map[string]metadata.Metadata{}).ChainLog(nil)
	appendLog(t, store, logTx)

	lastLog, err = store.GetLastLog(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lastLog)

	require.Equal(t, tx1.Postings, lastLog.Data.(ledger.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx1.Reference, lastLog.Data.(ledger.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx1.Timestamp, lastLog.Data.(ledger.NewTransactionLogPayload).Transaction.Timestamp)
}

func TestReadLogWithIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	logTx := ledger.NewTransactionLog(
		ledger.NewTransaction().
			WithPostings(
				ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
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
	now := ledger.Now()

	tx1 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(0),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
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
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
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
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
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
	tx2 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(1),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
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
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
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
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
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
	tx3 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(2),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
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
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
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
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
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

	var previousLog *ledger.ChainedLog
	for _, tx := range []ledger.ExpandedTransaction{tx1, tx2, tx3} {
		newLog := ledger.NewTransactionLog(&tx.Transaction, map[string]metadata.Metadata{}).
			WithDate(tx.Timestamp).
			ChainLog(previousLog)
		appendLog(t, store, newLog)
		previousLog = newLog
	}

	cursor, err := store.GetLogs(context.Background(), ledgerstore.NewGetLogsQuery(ledgerstore.NewPaginatedQueryOptions[any](nil)))
	require.NoError(t, err)
	require.Equal(t, paginate.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.Equal(t, big.NewInt(2), cursor.Data[0].ID)
	require.Equal(t, tx3.Postings, cursor.Data[0].Data.(ledger.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx3.Reference, cursor.Data[0].Data.(ledger.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx3.Timestamp, cursor.Data[0].Data.(ledger.NewTransactionLogPayload).Transaction.Timestamp)

	cursor, err = store.GetLogs(context.Background(), ledgerstore.NewGetLogsQuery(ledgerstore.NewPaginatedQueryOptions[any](nil).WithPageSize(1)))
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.Equal(t, big.NewInt(2), cursor.Data[0].ID)

	cursor, err = store.GetLogs(context.Background(), ledgerstore.NewGetLogsQuery(ledgerstore.NewPaginatedQueryOptions[any](nil).
		WithQueryBuilder(query.And(
			query.Gte("date", now.Add(-2*time.Hour)),
			query.Lt("date", now.Add(-time.Hour)),
		)).
		WithPageSize(10),
	))
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.Equal(t, big.NewInt(1), cursor.Data[0].ID)
}

func TestGetBalance(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	const (
		batchNumber = 100
		batchSize   = 10
		input       = 100
		output      = 10
	)

	logs := make([]*ledger.ChainedLog, 0)
	var previousLog *ledger.ChainedLog
	for i := 0; i < batchNumber; i++ {
		for j := 0; j < batchSize; j++ {
			chainedLog := ledger.NewTransactionLog(
				ledger.NewTransaction().WithPostings(
					ledger.NewPosting("world", fmt.Sprintf("account:%d", j), "EUR/2", big.NewInt(input)),
					ledger.NewPosting(fmt.Sprintf("account:%d", j), "starbucks", "EUR/2", big.NewInt(output)),
				).WithIDUint64(uint64(i*batchSize+j)),
				map[string]metadata.Metadata{},
			).ChainLog(previousLog)
			logs = append(logs, chainedLog)
			previousLog = chainedLog
		}
	}
	err := store.InsertLogs(context.Background(), logs...)
	require.NoError(t, err)

	balance, err := store.GetBalance(context.Background(), "account:1", "EUR/2")
	require.NoError(t, err)
	require.Equal(t, big.NewInt((input-output)*batchNumber), balance)
}
