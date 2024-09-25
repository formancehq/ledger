//go:build it

package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/go-libs/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/time"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	libtime "time"
)

func TestTransactionsGetWithVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "central_bank", "USD", big.NewInt(100)),
		).
		WithReference("tx1").
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "central_bank", "USD", big.NewInt(100)),
		).
		WithReference("tx2").
		WithTimestamp(now.Add(-2 * time.Hour))
	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	tx, err := store.GetTransaction(ctx, ledgercontroller.NewGetTransactionQuery(tx1.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)

	RequireEqual(t, ledger.PostCommitVolumes{
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
	}, tx.PostCommitVolumes)

	tx, err = store.GetTransaction(ctx, ledgercontroller.NewGetTransactionQuery(tx2.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.NoError(t, err)
	require.Equal(t, tx2.Postings, tx.Postings)
	require.Equal(t, tx2.Reference, tx.Reference)
	require.Equal(t, tx2.Timestamp, tx.Timestamp)
	RequireEqual(t, ledger.PostCommitVolumes{
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
	}, tx.PostCommitVolumes)
}

func TestTransactionsCount(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	for i := 0; i < 3; i++ {
		tx := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("world", fmt.Sprintf("account%d", i), "USD", big.NewInt(100)),
		)
		err := store.CommitTransaction(ctx, &tx)
		require.NoError(t, err)
	}

	count, err := store.CountTransactions(ctx, ledgercontroller.NewListTransactionsQuery(ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{})))
	require.NoError(t, err, "counting transactions should not fail")
	require.Equal(t, 3, count, "count should be equal")
}

func TestTransactionUpdateMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// create some transactions
	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "polo", "USD", big.NewInt(200)),
		).
		WithTimestamp(now.Add(-2 * time.Hour))
	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	// update their metadata
	_, modified, err := store.UpdateTransactionMetadata(ctx, tx1.ID, metadata.Metadata{"foo1": "bar2"})
	require.NoError(t, err)
	require.True(t, modified)

	_, _, err = store.UpdateTransactionMetadata(ctx, tx2.ID, metadata.Metadata{"foo2": "bar2"})
	require.NoError(t, err)

	// check that the database returns metadata
	tx, err := store.GetTransaction(context.Background(), ledgercontroller.NewGetTransactionQuery(tx1.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar2"}, "metadata should be equal")

	tx, err = store.GetTransaction(context.Background(), ledgercontroller.NewGetTransactionQuery(tx2.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo2": "bar2"}, "metadata should be equal")

	// update metadata of a transaction already having those metadata
	_, modified, err = store.UpdateTransactionMetadata(ctx, tx1.ID, metadata.Metadata{"foo1": "bar2"})
	require.NoError(t, err)
	require.False(t, modified)

	// update metadata of non existing transactions
	_, modified, err = store.UpdateTransactionMetadata(ctx, 10, metadata.Metadata{"foo2": "bar2"})
	require.Error(t, err)
	require.True(t, errors.Is(err, postgres.ErrNotFound))
	require.False(t, modified)
}

func TestTransactionDeleteMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// create a tx with some metadata
	tx1 := pointer.For(ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"foo1": "bar1", "foo2": "bar2"}).
		WithTimestamp(now.Add(-3 * time.Hour)))
	err := store.CommitTransaction(ctx, tx1)
	require.NoError(t, err)

	// get from database and check metadata presence
	tx, err := store.GetTransaction(context.Background(), ledgercontroller.NewGetTransactionQuery(tx1.ID))
	require.NoError(t, err)
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar1", "foo2": "bar2"})

	// delete a metadata
	tx1, modified, err := store.DeleteTransactionMetadata(ctx, tx1.ID, "foo1")
	require.NoError(t, err)
	require.True(t, modified)

	tx, err = store.GetTransaction(context.Background(), ledgercontroller.NewGetTransactionQuery(tx1.ID))
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{"foo2": "bar2"}, tx.Metadata)

	// delete a not existing metadata
	tx1, modified, err = store.DeleteTransactionMetadata(ctx, tx1.ID, "foo1")
	require.NoError(t, err)
	require.False(t, modified)

	// delete metadata of a non existing transaction
	tx1, modified, err = store.DeleteTransactionMetadata(ctx, 10, "foo1")
	require.Error(t, err)
	require.True(t, errors.Is(err, postgres.ErrNotFound))
	require.False(t, modified)
}

func TestTransactionsCommit(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	t.Run("inserting some transactions", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)

		tx1 := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("account:1", "account:2", "USD", big.NewInt(100)),
		)
		err := store.CommitTransaction(ctx, &tx1)
		require.NoError(t, err)
		require.Equal(t, 1, tx1.ID)
		require.Equal(t, ledger.PostCommitVolumes{
			"account:1": ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"account:2": ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		}, tx1.PostCommitVolumes)
		require.Equal(t, tx1.PostCommitVolumes, tx1.PostCommitEffectiveVolumes)

		tx2 := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("account:2", "account:3", "USD", big.NewInt(100)),
		)
		err = store.CommitTransaction(ctx, &tx2)
		require.NoError(t, err)
		require.Equal(t, 2, tx2.ID)
		require.Equal(t, ledger.PostCommitVolumes{
			"account:2": ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(100),
					Output: big.NewInt(100),
				},
			},
			"account:3": ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		}, tx2.PostCommitVolumes)
		require.Equal(t, tx2.PostCommitVolumes, tx2.PostCommitEffectiveVolumes)
	})

	t.Run("triggering a deadlock should return appropriate postgres error", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)

		account1 := &Account{
			Address: "account:1",
		}
		_, err := store.upsertAccount(ctx, account1)
		require.NoError(t, err)

		account2 := &Account{
			Address: "account:2",
		}
		_, err = store.upsertAccount(ctx, account2)
		require.NoError(t, err)

		// todo: we should not need to update volumes to have a lock
		_, err = store.updateVolumes(ctx, AccountsVolumes{
			Ledger:      store.ledger.Name,
			Account:     "account:1",
			Asset:       "USD",
			Input:       big.NewInt(100),
			Output:      big.NewInt(0),
			AccountsSeq: account1.Seq,
		})
		require.NoError(t, err)

		_, err = store.updateVolumes(ctx, AccountsVolumes{
			Ledger:      store.ledger.Name,
			Account:     "account:2",
			Asset:       "USD",
			Input:       big.NewInt(100),
			Output:      big.NewInt(0),
			AccountsSeq: account2.Seq,
		})
		require.NoError(t, err)

		// Create a new sql transaction to commit a transaction from account:1 to account:2
		// it will block until storeWithBlockingTx is commited or rollbacked.
		txWithAccount1AsSource, err := store.db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = txWithAccount1AsSource.Rollback()
		})

		errorsChan := make(chan error, 2)

		storeWithTxWithAccount1AsSource := store.WithDB(txWithAccount1AsSource)
		unlockTx1Chan := make(chan chan struct{}, 1)
		go func() {
			// Simulate a transaction with bounded sources by asking for balances before calling CommitTransaction
			_, err := storeWithTxWithAccount1AsSource.GetBalances(ctx, ledgercontroller.BalanceQuery{
				"account:1": {"USD"},
			})
			require.NoError(t, err)

			ch := make(chan struct{})
			unlockTx1Chan <- ch
			<-ch

			errorsChan <- storeWithTxWithAccount1AsSource.CommitTransaction(
				ctx,
				pointer.For(ledger.NewTransaction().WithPostings(
					ledger.NewPosting("account:1", "account:2", "USD", big.NewInt(100)),
				)),
			)
		}()

		var unlockTx1 chan struct{}
		select {
		case unlockTx1 = <-unlockTx1Chan:
		case <-libtime.After(time.Second):
			require.Fail(t, "tx should have been started")
		}

		// Create a new sql transaction to commit a transaction from account:2 to account:1
		// it will block until storeWithBlockingTx is commited or rollbacked.
		txWithAccount2AsSource, err := store.db.BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = txWithAccount2AsSource.Rollback()
		})

		storeWithTxWithAccount2AsSource := store.WithDB(txWithAccount2AsSource)
		unlockTx2Chan := make(chan chan struct{}, 1)
		go func() {
			// Simulate a transaction with bounded sources by asking for balances before calling CommitTransaction
			_, err := storeWithTxWithAccount2AsSource.GetBalances(ctx, ledgercontroller.BalanceQuery{
				"account:2": {"USD"},
			})
			require.NoError(t, err)

			ch := make(chan struct{})
			unlockTx2Chan <- ch
			<-ch

			errorsChan <- storeWithTxWithAccount2AsSource.CommitTransaction(
				ctx,
				pointer.For(ledger.NewTransaction().WithPostings(
					ledger.NewPosting("account:2", "account:1", "USD", big.NewInt(100)),
				)),
			)
		}()

		var unlockTx2 chan struct{}
		select {
		case unlockTx2 = <-unlockTx2Chan:
		case <-libtime.After(time.Second):
			require.Fail(t, "tx should have been started")
		}

		// at this point, each sql transaction hold a RowExclusiveLock on balances tables on an account
		// unlocking them should trigger a deadlock
		close(unlockTx1)
		close(unlockTx2)

		select {
		case err := <-errorsChan:
			if err == nil {
				select {
				case err = <-errorsChan:
					if err == nil {
						require.Fail(t, "should have a deadlock")
					}
				case <-libtime.After(2 * time.Second):
					require.Fail(t, "transaction should have finished")
				}
			}
			require.True(t, errors.Is(err, postgres.ErrDeadlockDetected))
		case <-libtime.After(2 * time.Second):
			require.Fail(t, "transaction should have finished")
		}
	})
}

func TestInsertTransactionInPast(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithTimestamp(now)
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user1", "USD/2", big.NewInt(50)),
	).WithTimestamp(now.Add(time.Hour))

	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	// Insert in past must modify pre/post commit volumes of tx2
	tx3 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user2", "USD/2", big.NewInt(50)),
	).WithTimestamp(now.Add(30 * time.Minute))
	err = store.CommitTransaction(ctx, &tx3)
	require.NoError(t, err)

	// Insert before the oldest tx must update first_usage of involved account
	tx4 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithTimestamp(now.Add(-time.Minute))
	err = store.CommitTransaction(ctx, &tx4)
	require.NoError(t, err)

	tx2FromDatabase, err := store.GetTransaction(ctx, ledgercontroller.NewGetTransactionQuery(tx2.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	RequireEqual(t, ledger.PostCommitVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(200, 100),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(50, 0),
		},
	}, tx2FromDatabase.PostCommitEffectiveVolumes)

	account, err := store.GetAccount(ctx, ledgercontroller.NewGetAccountQuery("bank"))
	require.NoError(t, err)
	require.Equal(t, tx4.Timestamp, account.FirstUsage)
}

func TestTransactionsRevert(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// create a simple tx
	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "1"}).
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	// revert the tx
	revertedTx, reverted, err := store.RevertTransaction(ctx, tx1.ID)
	require.NoError(t, err)
	require.True(t, reverted)
	require.NotNil(t, revertedTx)
	require.True(t, revertedTx.Reverted)
	revertedTx.Reverted = false
	tx1.PostCommitEffectiveVolumes = nil
	//tx1.PostCommitVolumes = ledger.PostCommitVolumes{}
	require.Equal(t, tx1, *revertedTx)

	// try to revert again
	revertedTx, reverted, err = store.RevertTransaction(ctx, tx1.ID)
	require.NoError(t, err)
	require.False(t, reverted)

	// revert a not existing transaction
	revertedTx, reverted, err = store.RevertTransaction(ctx, 2)
	require.True(t, errors.Is(err, postgres.ErrNotFound))
	require.False(t, reverted)
	require.Nil(t, revertedTx)
}

func TestTransactionsInsert(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// create a simple tx
	tx1 := Transaction{
		Ledger:    store.ledger.Name,
		Timestamp: now,
		Reference: "foo",
	}
	err := store.insertTransaction(ctx, &tx1)
	require.NoError(t, err)
	require.NotZero(t, tx1.ID)
	require.NotZero(t, tx1.Seq)

	// create another tx with the same reference
	tx2 := Transaction{
		Ledger:    store.ledger.Name,
		Timestamp: now,
		Reference: "foo",
	}
	err = store.insertTransaction(ctx, &tx2)
	require.Error(t, err)
	require.True(t, errors.Is(err, ledgercontroller.ErrReferenceConflict{}))

	// create a tx with no timestamp
	tx3 := Transaction{
		Ledger: store.ledger.Name,
	}
	err = store.insertTransaction(ctx, &tx3)
	require.NoError(t, err)
}

func TestTransactionsList(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "1"}).
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	tx2 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "bob", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "2"}).
		WithTimestamp(now.Add(-2 * time.Hour))
	err = store.CommitTransaction(ctx, &tx2)
	require.NoError(t, err)

	tx3BeforeRevert := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "3"}).
		WithTimestamp(now.Add(-time.Hour))
	err = store.CommitTransaction(ctx, &tx3BeforeRevert)
	require.NoError(t, err)

	_, hasBeenReverted, err := store.RevertTransaction(ctx, tx3BeforeRevert.ID)
	require.NoError(t, err)
	require.True(t, hasBeenReverted)

	tx4 := tx3BeforeRevert.Reverse(false).WithTimestamp(now)
	err = store.CommitTransaction(ctx, &tx4)
	require.NoError(t, err)

	_, _, err = store.UpdateTransactionMetadata(ctx, tx3BeforeRevert.ID, metadata.Metadata{
		"additional_metadata": "true",
	})

	// refresh tx3
	// we can't take the result of the call on RevertTransaction nor UpdateTransactionMetadata as the result does not contains pc(e)v
	tx3 := func() ledger.Transaction {
		tx3, err := store.GetTransaction(ctx, ledgercontroller.NewGetTransactionQuery(tx3BeforeRevert.ID).
			WithExpandVolumes().
			WithExpandEffectiveVolumes())
		require.NoError(t, err)
		return *tx3
	}()

	tx5 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("users:marley", "sellers:amazon", "USD", big.NewInt(100)),
		).
		WithTimestamp(now)
	err = store.CommitTransaction(ctx, &tx5)
	require.NoError(t, err)

	type testCase struct {
		name        string
		query       ledgercontroller.PaginatedQueryOptions[ledgercontroller.PITFilterWithVolumes]
		expected    []ledger.Transaction
		expectError error
	}
	testCases := []testCase{
		{
			name:     "nominal",
			query:    ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}),
			expected: []ledger.Transaction{tx5, tx4, tx3, tx2, tx1},
		},
		{
			name: "address filter",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "bob")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "address filter using segments matching two addresses by individual segments",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "users:amazon")),
			expected: []ledger.Transaction{},
		},
		{
			name: "address filter using segment",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "users:")),
			expected: []ledger.Transaction{tx5, tx4, tx3},
		},
		{
			name: "filter using metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[category]", "2")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "using point in time",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: pointer.For(now.Add(-time.Hour)),
				},
			}),
			expected: []ledger.Transaction{tx3BeforeRevert, tx2, tx1},
		},
		{
			name: "filter using invalid key",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("invalid", "2")),
			expectError: ledgercontroller.ErrInvalidQuery{},
		},
		{
			name: "reverted transactions",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("reverted", true)),
			expected: []ledger.Transaction{tx3},
		},
		{
			name: "filter using exists metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Exists("metadata", "category")),
			expected: []ledger.Transaction{tx3, tx2, tx1},
		},
		{
			name: "filter using exists metadata and pit",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{
				PITFilter: ledgercontroller.PITFilter{
					PIT: pointer.For(tx3.Timestamp),
				},
			}).
				WithQueryBuilder(query.Match("metadata[category]", "2")),
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "filter using not exists metadata",
			query: ledgercontroller.NewPaginatedQueryOptions(ledgercontroller.PITFilterWithVolumes{}).
				WithQueryBuilder(query.Not(query.Exists("metadata", "category"))),
			expected: []ledger.Transaction{tx5, tx4},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.query.Options.ExpandVolumes = true
			tc.query.Options.ExpandEffectiveVolumes = true

			cursor, err := store.ListTransactions(ctx, ledgercontroller.NewListTransactionsQuery(tc.query))
			if tc.expectError != nil {
				require.True(t, errors.Is(err, tc.expectError))
			} else {
				require.NoError(t, err)
				require.Len(t, cursor.Data, len(tc.expected))
				RequireEqual(t, tc.expected, cursor.Data)

				count, err := store.CountTransactions(ctx, ledgercontroller.NewListTransactionsQuery(tc.query))
				require.NoError(t, err)

				require.EqualValues(t, len(tc.expected), count)
			}
		})
	}
}
