package ledger_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestTransactions(t *testing.T) {
	store := newLedgerStore(t)

	var (
		tx1, tx2, tx3 core.ExpandedTransaction
	)

	t.Run("success inserting transaction", func(t *testing.T) {
		tx1 = core.ExpandedTransaction{
			Transaction: core.NewTransaction().
				WithPostings(
					core.NewPosting("world", "alice", "USD", big.NewInt(100)),
				).
				WithTimestamp(now.Add(-3 * time.Hour)),
		}

		err := store.InsertTransactions(context.Background(), tx1)
		require.NoError(t, err, "inserting transaction should not fail")

		tx, err := store.GetTransaction(context.Background(), tx1.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx1, tx, "transaction should be equal")
	})

	t.Run("success inserting multiple transactions", func(t *testing.T) {
		tx2 = core.ExpandedTransaction{
			Transaction: core.NewTransaction().
				WithPostings(
					core.NewPosting("world", "polo", "USD", big.NewInt(100)),
				).
				WithTimestamp(now.Add(-2 * time.Hour)),
		}

		tx3 = core.ExpandedTransaction{
			Transaction: core.NewTransaction().
				WithPostings(
					core.NewPosting("world", "gfyrag", "USD", big.NewInt(150)),
				).
				WithTimestamp(now.Add(-1 * time.Hour)),
		}

		err := store.InsertTransactions(context.Background(), tx2, tx3)
		require.NoError(t, err, "inserting multiple transactions should not fail")

		tx, err := store.GetTransaction(context.Background(), tx2.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx2, tx, "transaction should be equal")

		tx, err = store.GetTransaction(context.Background(), tx3.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, &tx3, tx, "transaction should be equal")
	})

	t.Run("success counting transactions", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
		require.NoError(t, err, "counting transactions should not fail")
		require.Equal(t, uint64(3), count, "count should be equal")
	})

	t.Run("success updating transaction metadata", func(t *testing.T) {
		metadata := metadata.Metadata{
			"foo": "bar",
		}
		err := store.UpdateTransactionMetadata(context.Background(), tx1.ID, metadata)
		require.NoError(t, err, "updating transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), tx1.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, metadata, "metadata should be equal")
	})

	t.Run("success updating multiple transaction metadata", func(t *testing.T) {
		txToUpdate1 := core.TransactionWithMetadata{
			ID:       tx2.ID,
			Metadata: metadata.Metadata{"foo1": "bar2"},
		}
		txToUpdate2 := core.TransactionWithMetadata{
			ID:       tx3.ID,
			Metadata: metadata.Metadata{"foo2": "bar2"},
		}
		txs := []core.TransactionWithMetadata{txToUpdate1, txToUpdate2}

		err := store.UpdateTransactionsMetadata(context.Background(), txs...)
		require.NoError(t, err, "updating multiple transaction metadata should not fail")

		tx, err := store.GetTransaction(context.Background(), txToUpdate1.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, txToUpdate1.Metadata, "metadata should be equal")

		tx, err = store.GetTransaction(context.Background(), txToUpdate2.ID)
		require.NoError(t, err, "getting transaction should not fail")
		require.Equal(t, tx.Metadata, txToUpdate2.Metadata, "metadata should be equal")
	})
}
