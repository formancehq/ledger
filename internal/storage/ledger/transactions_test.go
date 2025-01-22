//go:build it

package ledger_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/alitto/pond"
	"math/big"
	"slices"
	"testing"

	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"

	libtime "time"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
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

	tx, err := store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx1.ID),
		Expand:  []string{"volumes", "effectiveVolumes"},
	})
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

	tx, err = store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx2.ID),
		Expand:  []string{"volumes", "effectiveVolumes"},
	})
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

	count, err := store.Transactions().Count(ctx, ledgercontroller.ResourceQuery[any]{})
	require.NoError(t, err, "counting transactions should not fail")
	require.Equal(t, 3, count, "count should be equal")
}

func TestTransactionUpdateMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// Create some transactions
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

	// Update their metadata
	_, modified, err := store.UpdateTransactionMetadata(ctx, tx1.ID, metadata.Metadata{"foo1": "bar2"})
	require.NoError(t, err)
	require.True(t, modified)

	_, _, err = store.UpdateTransactionMetadata(ctx, tx2.ID, metadata.Metadata{"foo2": "bar2"})
	require.NoError(t, err)

	// Check that the database returns metadata
	tx, err := store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx1.ID),
		Expand:  []string{"volumes", "effectiveVolumes"},
	})
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar2"}, "metadata should be equal")

	tx, err = store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx2.ID),
		Expand:  []string{"volumes", "effectiveVolumes"},
	})
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo2": "bar2"}, "metadata should be equal")

	// Update metadata of a transaction already having those metadata
	_, modified, err = store.UpdateTransactionMetadata(ctx, tx1.ID, metadata.Metadata{"foo1": "bar2"})
	require.NoError(t, err)
	require.False(t, modified)

	// Update metadata of non existing transactions
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

	// Create a tx with some metadata
	tx1 := pointer.For(ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"foo1": "bar1", "foo2": "bar2"}).
		WithTimestamp(now.Add(-3 * time.Hour)))
	err := store.CommitTransaction(ctx, tx1)
	require.NoError(t, err)

	// Get from database and check metadata presence
	tx, err := store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx1.ID),
	})
	require.NoError(t, err)
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar1", "foo2": "bar2"})

	// Delete a metadata
	tx1, modified, err := store.DeleteTransactionMetadata(ctx, tx1.ID, "foo1")
	require.NoError(t, err)
	require.True(t, modified)

	tx, err = store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx1.ID),
	})
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{"foo2": "bar2"}, tx.Metadata)

	// Delete a not existing metadata
	_, modified, err = store.DeleteTransactionMetadata(ctx, tx1.ID, "foo1")
	require.NoError(t, err)
	require.False(t, modified)

	// Delete metadata of a non existing transaction
	_, modified, err = store.DeleteTransactionMetadata(ctx, 10, "foo1")
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

	t.Run("auto send", func(t *testing.T) {
		store := newLedgerStore(t)

		tx3 := ledger.NewTransaction().WithPostings(
			ledger.NewPosting("account:x", "account:x", "USD", big.NewInt(100)),
		)
		err := store.CommitTransaction(ctx, &tx3)
		require.NoError(t, err)
		require.Equal(t, 1, tx3.ID)
		require.Equal(t, ledger.PostCommitVolumes{
			"account:x": ledger.VolumesByAssets{
				"USD": ledger.Volumes{
					Input:  big.NewInt(100),
					Output: big.NewInt(100),
				},
			},
		}, tx3.PostCommitVolumes)
		require.Equal(t, tx3.PostCommitVolumes, tx3.PostCommitEffectiveVolumes)
	})

	t.Run("triggering a deadlock should return appropriate postgres error", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)

		// Create a new sql transaction to commit a transaction from account:1 to account:2.
		// It will block until storeWithBlockingTx is commited or rollbacked.
		txWithAccount1AsSource, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = txWithAccount1AsSource.Rollback()
		})

		errorsChan := make(chan error, 2)

		storeWithTxWithAccount1AsSource := store.WithDB(txWithAccount1AsSource)
		unlockTx1Chan := make(chan chan struct{}, 1)
		tx1Context, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			// Simulate a transaction with bounded sources by asking for balances before calling CommitTransaction
			_, err := storeWithTxWithAccount1AsSource.GetBalances(tx1Context, ledgercontroller.BalanceQuery{
				"account:1": {"USD"},
			})
			require.NoError(t, err)

			ch := make(chan struct{})
			unlockTx1Chan <- ch
			<-ch

			errorsChan <- storeWithTxWithAccount1AsSource.CommitTransaction(
				tx1Context,
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

		// Create a new sql transaction to commit a transaction from account:2 to account:1.
		// It will block until storeWithBlockingTx is commited or rollbacked.
		txWithAccount2AsSource, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = txWithAccount2AsSource.Rollback()
		})

		storeWithTxWithAccount2AsSource := store.WithDB(txWithAccount2AsSource)
		unlockTx2Chan := make(chan chan struct{}, 1)
		tx2Context, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		go func() {
			// Simulate a transaction with bounded sources by asking for balances before calling CommitTransaction
			_, err := storeWithTxWithAccount2AsSource.GetBalances(tx2Context, ledgercontroller.BalanceQuery{
				"account:2": {"USD"},
			})
			require.NoError(t, err)

			ch := make(chan struct{})
			unlockTx2Chan <- ch
			<-ch

			errorsChan <- storeWithTxWithAccount2AsSource.CommitTransaction(
				tx2Context,
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

		// At this point, each sql transaction hold a RowExclusiveLock on balances tables on an account.
		// Unlocking them should trigger a deadlock.
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

	t.Run("post commit volumes ordering on concurrent transactions", func(t *testing.T) {
		t.Parallel()

		const countTx = 100
		store := newLedgerStore(t)

		errChan := make(chan error, countTx)
		wp := pond.New(20, 20)
		for i := 0; i < countTx; i++ {
			wp.Submit(func() {

				sqlTX, err := store.GetDB().BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					errChan <- err
					return
				}
				defer func() {
					_ = sqlTX.Rollback()
				}()
				store := store.WithDB(sqlTX)

				tx := ledger.NewTransaction().WithPostings(
					ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
				)
				err = store.CommitTransaction(ctx, &tx)
				if err != nil {
					errChan <- err
					return
				}

				err = sqlTX.Commit()
				if err != nil {
					errChan <- err
					return
				}

				errChan <- nil
			})
		}
		wp.StopAndWait()
		close(errChan)

		for err := range errChan {
			require.NoError(t, err)
		}

		cursor, err := store.Transactions().Paginate(ctx, ledgercontroller.ColumnPaginatedQuery[any]{
			PageSize: countTx,
			Options: ledgercontroller.ResourceQuery[any]{
				Expand: []string{"volumes"},
			},
		})
		require.NoError(t, err)
		require.Len(t, cursor.Data, countTx)

		txs := cursor.Data
		slices.Reverse(txs)

		for i := range countTx {
			require.Equal(t, i+1, txs[i].ID)
			require.Equalf(t, ledger.PostCommitVolumes{
				"world": {
					"USD": {
						Input:  big.NewInt(0),
						Output: big.NewInt(int64((i + 1) * 100)),
					},
				},
				"bank": {
					"USD": {
						Input:  big.NewInt(int64((i + 1) * 100)),
						Output: big.NewInt(0),
					},
				},
			}, txs[i].PostCommitVolumes, "checking tx %d", i)
			if i > 0 {
				require.Truef(t, txs[i].InsertedAt.After(txs[i-1].InsertedAt), "checking tx %d", i)
			}
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

	tx2FromDatabase, err := store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("id", tx2.ID),
		Expand:  []string{"volumes", "effectiveVolumes"},
	})
	require.NoError(t, err)

	RequireEqual(t, ledger.PostCommitVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(200, 100),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(50, 0),
		},
	}, tx2FromDatabase.PostCommitEffectiveVolumes)

	account, err := store.Accounts().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
		Builder: query.Match("address", "bank"),
	})
	require.NoError(t, err)
	require.Equal(t, tx4.Timestamp, account.FirstUsage)
}

func TestTransactionsRevert(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	// Create a simple tx
	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "1"}).
		WithTimestamp(now.Add(-3 * time.Hour))
	err := store.CommitTransaction(ctx, &tx1)
	require.NoError(t, err)

	// Revert the tx
	revertedTx, reverted, err := store.RevertTransaction(ctx, tx1.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, reverted)
	require.NotNil(t, revertedTx)
	require.True(t, revertedTx.IsReverted())
	revertedTx.RevertedAt = nil
	// As the RevertTransaction method does not return post commit effective volumes,
	// we remove them to be able to compare revertedTx with tx1
	tx1.PostCommitEffectiveVolumes = nil
	require.Equal(t, tx1, *revertedTx)

	// Try to revert again
	_, reverted, err = store.RevertTransaction(ctx, tx1.ID, time.Time{})
	require.NoError(t, err)
	require.False(t, reverted)

	// Revert a not existing transaction
	_, reverted, err = store.RevertTransaction(ctx, 2, time.Time{})
	require.True(t, errors.Is(err, postgres.ErrNotFound))
	require.False(t, reverted)
}

func TestTransactionsInsert(t *testing.T) {
	t.Parallel()

	now := time.Now()
	ctx := logging.TestingContext()

	t.Run("check reference conflict", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)

		// Create a simple tx
		tx1 := ledger.Transaction{
			TransactionData: ledger.TransactionData{
				Timestamp: now,
				Reference: "foo",
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
				},
			},
		}
		err := store.InsertTransaction(ctx, &tx1)
		require.NoError(t, err)
		require.NotZero(t, tx1.ID)

		// Create another tx with the same reference
		tx2 := ledger.Transaction{
			TransactionData: ledger.TransactionData{
				Timestamp: now,
				Reference: "foo",
				Postings: []ledger.Posting{
					ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
				},
			},
		}
		err = store.InsertTransaction(ctx, &tx2)
		require.Error(t, err)
		require.True(t, errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}))
	})
	t.Run("check denormalization", func(t *testing.T) {
		t.Parallel()

		store := newLedgerStore(t)

		tx1 := ledger.NewTransaction().
			WithPostings(
				ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
			).
			WithInsertedAt(now).
			WithTimestamp(now)

		err := store.InsertTransaction(ctx, &tx1)
		require.NoError(t, err)

		type Model struct {
			ledger.Transaction
			Sources            []string         `bun:"sources,type:jsonb"`
			Destinations       []string         `bun:"destinations,type:jsonb"`
			SourcesArrays      []map[string]any `bun:"sources_arrays,type:jsonb"`
			DestinationsArrays []map[string]any `bun:"destinations_arrays,type:jsonb"`
		}

		m := Model{}
		err = store.GetDB().
			NewSelect().
			Model(&m).
			ModelTableExpr(store.GetPrefixedRelationName("transactions")+" as model").
			Where("ledger = ?", store.GetLedger().Name).
			Scan(ctx)
		require.NoError(t, err)
		require.Equal(t, Model{
			Transaction:  tx1,
			Sources:      []string{"world"},
			Destinations: []string{"bank"},
			SourcesArrays: []map[string]any{{
				"0": "world",
				"1": nil,
			}},
			DestinationsArrays: []map[string]any{{
				"0": "bank",
				"1": nil,
			}},
		}, m)
	})
}

func TestTransactionsList(t *testing.T) {
	t.Parallel()

	store := newLedgerStore(t)
	now := time.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
			ledger.NewPosting("world", "alice", "EUR", big.NewInt(100)),
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

	_, hasBeenReverted, err := store.RevertTransaction(ctx, tx3BeforeRevert.ID, time.Time{})
	require.NoError(t, err)
	require.True(t, hasBeenReverted)

	tx4 := tx3BeforeRevert.Reverse().WithTimestamp(now)
	err = store.CommitTransaction(ctx, &tx4)
	require.NoError(t, err)

	_, _, err = store.UpdateTransactionMetadata(ctx, tx3BeforeRevert.ID, metadata.Metadata{
		"additional_metadata": "true",
	})
	require.NoError(t, err)

	// refresh tx3
	// we can't take the result of the call on RevertTransaction nor UpdateTransactionMetadata as the result does not contains pc(e)v
	tx3 := func() ledger.Transaction {
		tx3, err := store.Transactions().GetOne(ctx, ledgercontroller.ResourceQuery[any]{
			Builder: query.Match("id", tx3BeforeRevert.ID),
			Expand:  []string{"volumes", "effectiveVolumes"},
		})
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
		query       ledgercontroller.ColumnPaginatedQuery[any]
		expected    []ledger.Transaction
		expectError error
	}
	testCases := []testCase{
		{
			name:     "nominal",
			query:    ledgercontroller.ColumnPaginatedQuery[any]{},
			expected: []ledger.Transaction{tx5, tx4, tx3, tx2, tx1},
		},
		{
			name: "address filter",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("account", "bob"),
				},
			},
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "address filter using segments matching two addresses by individual segments",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("account", "users:amazon"),
				},
			},
			expected: []ledger.Transaction{},
		},
		{
			name: "address filter using segment",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("account", "users:"),
				},
			},
			expected: []ledger.Transaction{tx5, tx4, tx3},
		},
		{
			name: "filter using metadata",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("metadata[category]", "2"),
				},
			},
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "using point in time",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					PIT: pointer.For(now.Add(-time.Hour)),
				},
			},
			expected: []ledger.Transaction{tx3BeforeRevert, tx2, tx1},
		},
		{
			name: "filter using invalid key",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("invalid", "2"),
				},
			},
			expectError: ledgercontroller.ErrInvalidQuery{},
		},
		{
			name: "reverted transactions",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("reverted", true),
				},
			},
			expected: []ledger.Transaction{tx3},
		},
		{
			name: "filter using exists metadata",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Exists("metadata", "category"),
				},
			},
			expected: []ledger.Transaction{tx3, tx2, tx1},
		},
		{
			name: "filter using metadata and pit",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("metadata[category]", "2"),
					PIT:     pointer.For(tx3.Timestamp),
				},
			},
			expected: []ledger.Transaction{tx2},
		},
		{
			name: "filter using not exists metadata",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Not(query.Exists("metadata", "category")),
				},
			},
			expected: []ledger.Transaction{tx5, tx4},
		},
		{
			name: "filter using timestamp",
			query: ledgercontroller.ColumnPaginatedQuery[any]{
				Options: ledgercontroller.ResourceQuery[any]{
					Builder: query.Match("timestamp", tx5.Timestamp.Format(time.RFC3339Nano)),
				},
			},
			expected: []ledger.Transaction{tx5, tx4},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.query.Options.Expand = []string{"volumes", "effectiveVolumes"}

			cursor, err := store.Transactions().Paginate(ctx, tc.query)
			if tc.expectError != nil {
				require.True(t, errors.Is(err, tc.expectError))
			} else {
				require.NoError(t, err)
				require.Len(t, cursor.Data, len(tc.expected))
				RequireEqual(t, tc.expected, cursor.Data)

				count, err := store.Transactions().Count(ctx, tc.query.Options)
				require.NoError(t, err)

				require.EqualValues(t, len(tc.expected), count)
			}
		})
	}
}
