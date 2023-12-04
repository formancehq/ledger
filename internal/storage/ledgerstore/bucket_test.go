package ledgerstore

import (
	"math/big"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuckets(t *testing.T) {
	ctx := logging.TestingContext()
	bucket := newBucket(t)
	var (
		ledger0 = uuid.NewString()
		ledger1 = uuid.NewString()
	)
	ledger0Store, err := bucket.CreateLedgerStore(ledger0)
	require.NoError(t, err)

	ledger1Store, err := bucket.CreateLedgerStore(ledger1)
	require.NoError(t, err)

	txLedger0 := ledger.Transaction{
		ID: big.NewInt(0),
		TransactionData: ledger.TransactionData{
			Postings: ledger.Postings{
				{
					Source:      "world",
					Destination: "alice",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Metadata: metadata.Metadata{},
		},
	}

	txLedger1 := ledger.Transaction{
		ID: big.NewInt(0),
		TransactionData: ledger.TransactionData{
			Postings: ledger.Postings{
				{
					Source:      "world",
					Destination: "alice",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Metadata: metadata.Metadata{},
		},
	}

	require.NoError(t, ledger0Store.InsertLogs(ctx,
		ledger.NewTransactionLog(&txLedger0, map[string]metadata.Metadata{}).ChainLog(nil),
	))
	require.NoError(t, ledger1Store.InsertLogs(ctx,
		ledger.NewTransactionLog(&txLedger1, map[string]metadata.Metadata{}).ChainLog(nil),
	))

	count, err := ledger0Store.CountTransactions(ctx, NewGetTransactionsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{}))
	require.NoError(t, err)
	require.Equal(t, count, 1)

	count, err = ledger1Store.CountTransactions(ctx, NewGetTransactionsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{}))
	require.NoError(t, err)
	require.Equal(t, count, 1)
}
