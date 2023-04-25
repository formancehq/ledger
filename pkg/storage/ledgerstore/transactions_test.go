package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestTransactions(t *testing.T) {
	store := newLedgerStore(t)

	t.Run("success inserting transaction", func(t *testing.T) {
		tx1 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 0,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      big.NewInt(100),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-3 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
		}

		err := store.InsertTransactions(context.Background(), tx1)
		require.NoError(t, err, "inserting transaction should not fail")

		tx, err := store.GetTransaction(context.Background(), 0)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx1, tx, "transaction should be equal")
	})

	t.Run("success inserting multiple transactions", func(t *testing.T) {
		tx2 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 1,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "polo",
							Amount:      big.NewInt(200),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-2 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
		}

		tx3 := core.ExpandedTransaction{
			Transaction: core.Transaction{
				ID: 2,
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "gfyrag",
							Amount:      big.NewInt(150),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-1 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
		}

		err := store.InsertTransactions(context.Background(), tx2, tx3)
		require.NoError(t, err, "inserting multiple transactions should not fail")

		tx, err := store.GetTransaction(context.Background(), 1)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx2, tx, "transaction should be equal")

		tx, err = store.GetTransaction(context.Background(), 2)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx3, tx, "transaction should be equal")
	})

	t.Run("success counting transactions", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), ledgerstore.TransactionsQuery{})
		require.NoError(t, err, "counting transactions should not fail")
		require.Equal(t, uint64(3), count, "count should be equal")
	})

	t.Run("success updating transaction metadata", func(t *testing.T) {
		metadata := metadata.Metadata{
			"foo": "bar",
		}
		err := store.UpdateTransactionMetadata(context.Background(), 0, metadata)
		require.NoError(t, err, "updating transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), 0)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, metadata, "metadata should be equal")
	})

	t.Run("success updating multiple transaction metadata", func(t *testing.T) {
		txToUpdate1 := core.TransactionWithMetadata{
			ID:       1,
			Metadata: metadata.Metadata{"foo1": "bar2"},
		}
		txToUpdate2 := core.TransactionWithMetadata{
			ID:       2,
			Metadata: metadata.Metadata{"foo2": "bar2"},
		}
		txs := []core.TransactionWithMetadata{txToUpdate1, txToUpdate2}

		err := store.UpdateTransactionsMetadata(context.Background(), txs...)
		require.NoError(t, err, "updating multiple transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), 1)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, txToUpdate1.Metadata, "metadata should be equal")

		tx, err = store.GetTransaction(context.Background(), 2)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, txToUpdate2.Metadata, "metadata should be equal")
	})
}

func TestListTransactions(t *testing.T) {
	store := newLedgerStore(t)

	tx1 := core.ExpandTransactionFromEmptyPreCommitVolumes(
		core.NewTransaction().
			WithID(0).
			WithPostings(
				core.NewPosting("world", "alice", "USD", big.NewInt(100)),
			).
			WithTimestamp(now.Add(-3 * time.Hour)),
	)
	tx2 := core.ExpandTransactionFromEmptyPreCommitVolumes(
		core.NewTransaction().
			WithID(1).
			WithPostings(
				core.NewPosting("world", "bob", "USD", big.NewInt(100)),
			).
			WithTimestamp(now.Add(-2 * time.Hour)),
	)
	tx3 := core.ExpandTransactionFromEmptyPreCommitVolumes(
		core.NewTransaction().
			WithID(2).
			WithPostings(
				core.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
			).
			WithTimestamp(now.Add(-time.Hour)),
	)

	require.NoError(t, store.InsertTransactions(context.Background(), tx1, tx2, tx3))

	type testCase struct {
		name     string
		query    ledgerstore.TransactionsQuery
		expected *api.Cursor[core.ExpandedTransaction]
	}
	testCases := []testCase{
		{
			name:  "nominal",
			query: ledgerstore.NewTransactionsQuery(),
			expected: &api.Cursor[core.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     []core.ExpandedTransaction{tx3, tx2, tx1},
			},
		},
		{
			name: "address filter",
			query: ledgerstore.NewTransactionsQuery().
				WithAccountFilter("bob"),
			expected: &api.Cursor[core.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     []core.ExpandedTransaction{tx2},
			},
		},
		{
			name: "address filter using segment",
			query: ledgerstore.NewTransactionsQuery().
				WithAccountFilter("users:"),
			expected: &api.Cursor[core.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     []core.ExpandedTransaction{tx3},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := store.GetTransactions(context.Background(), tc.query)
			require.NoError(t, err)
			require.Equal(t, *tc.expected, *cursor)

			count, err := store.CountTransactions(context.Background(), tc.query)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.expected.Data), count)
		})
	}
}
