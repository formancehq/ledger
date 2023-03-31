package ledger_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/assert"
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
		assert.NoError(t, err, "inserting transaction should not fail")

		tx, err := store.GetTransaction(context.Background(), 0)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, &tx1, tx, "transaction should be equal")
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
		assert.NoError(t, err, "inserting multiple transactions should not fail")

		tx, err := store.GetTransaction(context.Background(), 1)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, &tx2, tx, "transaction should be equal")

		tx, err = store.GetTransaction(context.Background(), 2)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, &tx3, tx, "transaction should be equal")
	})

	t.Run("success counting transactions", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
		assert.NoError(t, err, "counting transactions should not fail")
		assert.Equal(t, uint64(3), count, "count should be equal")
	})

	t.Run("success updating transaction metadata", func(t *testing.T) {
		metadata := metadata.Metadata{
			"foo": "bar",
		}
		err := store.UpdateTransactionMetadata(context.Background(), 0, metadata)
		assert.NoError(t, err, "updating transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), 0)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, tx.Metadata, metadata, "metadata should be equal")
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
		assert.NoError(t, err, "updating multiple transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), 1)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, tx.Metadata, txToUpdate1.Metadata, "metadata should be equal")

		tx, err = store.GetTransaction(context.Background(), 2)
		assert.NoError(t, err, "getting transaction should not fail")
		assert.Equal(t, tx.Metadata, txToUpdate2.Metadata, "metadata should be equal")
	})
}
