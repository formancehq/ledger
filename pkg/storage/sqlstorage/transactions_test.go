package sqlstorage_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/google/uuid"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func BenchmarkStore_GetTransactions(b *testing.B) {
	b.StopTimer()

	invokeFunc := func(driver *sqlstorage.Driver, lc fx.Lifecycle) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				ledgerName := uuid.NewString()
				var store *sqlstorage.Store
				var err error
				for store == nil {
					store, _, err = driver.GetLedgerStore(ctx, ledgerName, true)
					if err != nil {
						fmt.Printf("sqlstorage.Driver.GetLedgerStore: %s\n", err.Error())
						time.Sleep(3 * time.Second)
					}
				}
				defer func(store ledger.Store, ctx context.Context) {
					require.NoError(b, store.Close(ctx))
				}(store, context.Background())

				if _, err = store.Initialize(context.Background()); err != nil {
					return err
				}

				benchGetTransactions(b, store)
				return nil
			},
		})
	}

	appSingleInstance := fx.New(
		fx.NopLogger,
		ledgertesting.ProvideStorageDriver(false),
		fx.Invoke(invokeFunc),
	)
	require.NoError(b, appSingleInstance.Start(context.Background()))
	defer func(app *fx.App, ctx context.Context) {
		require.NoError(b, app.Stop(ctx))
	}(appSingleInstance, context.Background())

	appMultipleInstance := fx.New(
		fx.NopLogger,
		ledgertesting.ProvideStorageDriver(true),
		fx.Invoke(invokeFunc),
	)
	require.NoError(b, appMultipleInstance.Start(context.Background()))
	defer func(app *fx.App, ctx context.Context) {
		require.NoError(b, app.Stop(ctx))
	}(appMultipleInstance, context.Background())
}

func benchGetTransactions(b *testing.B, store *sqlstorage.Store) {
	maxPages := 120
	maxPageSize := 500
	id := uint64(0)
	var txs []core.ExpandedTransaction
	for i := 0; i < maxPages; i++ {
		for j := 0; j < maxPageSize; j++ {
			acc := uuid.NewString() + ":key1:" + uuid.NewString() + ":key2:" + uuid.NewString()
			txs = append(txs, core.ExpandedTransaction{
				Transaction: core.Transaction{
					ID: id,
					TransactionData: core.TransactionData{
						Postings: []core.Posting{
							{
								Source:      acc,
								Destination: "world",
								Amount:      core.NewMonetaryInt(100),
								Asset:       "USD",
							},
						},
						Reference: uuid.NewString(),
						Timestamp: time.Now(),
					},
				},
				PostCommitVolumes: core.AccountsAssetsVolumes{
					"world": {
						"USD": {
							Input:  core.NewMonetaryInt(100),
							Output: core.NewMonetaryInt(0),
						},
					},
					acc: {
						"USD": {
							Input:  core.NewMonetaryInt(0),
							Output: core.NewMonetaryInt(100),
						},
					},
				},
				PreCommitVolumes: core.AccountsAssetsVolumes{
					"world": {
						"USD": {
							Input:  core.NewMonetaryInt(0),
							Output: core.NewMonetaryInt(0),
						},
					},
					acc: {
						"USD": {
							Input:  core.NewMonetaryInt(0),
							Output: core.NewMonetaryInt(0),
						},
					},
				},
			})
			id++
		}
		if len(txs) >= 1000 {
			require.NoError(b, store.Commit(context.Background(), txs...))
			txs = []core.ExpandedTransaction{}
		}
	}
	if len(txs) > 0 {
		require.NoError(b, store.Commit(context.Background(), txs...))
	}

	numTxs := maxPages * maxPageSize
	nb, err := store.CountTransactions(context.Background(), ledger.TransactionsQuery{})
	require.NoError(b, err)
	require.Equal(b, uint64(numTxs), nb)

	firstQ1, midQ1, lastQ1 := getTxQueries(b, store, 1, maxPages*maxPageSize)
	firstQ50, midQ50, lastQ50 := getTxQueries(b, store, 50, maxPages*maxPageSize)
	firstQ500, midQ500, lastQ500 := getTxQueries(b, store, 500, maxPages*maxPageSize)
	var cursor api.Cursor[core.ExpandedTransaction]

	b.ResetTimer()
	b.StartTimer()

	b.Run("firstQ1", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *firstQ1)
			require.NoError(b, err)
		}
		require.Equal(b, 1, cursor.PageSize)
		require.Len(b, cursor.Data, 1)
		require.Equal(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("midQ1", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *midQ1)
			require.NoError(b, err)
		}
		require.Equal(b, 1, cursor.PageSize)
		require.Len(b, cursor.Data, 1)
		require.NotEqual(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("lastQ1", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *lastQ1)
			require.NoError(b, err)
		}
		require.Equal(b, 1, cursor.PageSize)
		require.Len(b, cursor.Data, 1)
		require.NotEqual(b, "", cursor.Previous)
		require.Equal(b, "", cursor.Next)
	})

	b.Run("firstQ50", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *firstQ50)
			require.NoError(b, err)
		}
		require.Equal(b, 50, cursor.PageSize)
		require.Len(b, cursor.Data, 50)
		require.Equal(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("midQ50", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *midQ50)
			require.NoError(b, err)
		}
		require.Equal(b, 50, cursor.PageSize)
		require.Len(b, cursor.Data, 50)
		require.NotEqual(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("lastQ50", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *lastQ50)
			require.NoError(b, err)
		}
		require.Equal(b, 50, cursor.PageSize)
		require.Len(b, cursor.Data, 50)
		require.NotEqual(b, "", cursor.Previous)
		require.Equal(b, "", cursor.Next)
	})

	b.Run("firstQ500", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *firstQ500)
			require.NoError(b, err)
		}
		require.Equal(b, 500, cursor.PageSize)
		require.Len(b, cursor.Data, 500)
		require.Equal(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("midQ500", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *midQ500)
			require.NoError(b, err)
		}
		require.Equal(b, 500, cursor.PageSize)
		require.Len(b, cursor.Data, 500)
		require.NotEqual(b, "", cursor.Previous)
		require.NotEqual(b, "", cursor.Next)
	})

	b.Run("lastQ500", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			cursor, err = store.GetTransactions(context.Background(), *lastQ500)
			require.NoError(b, err)
		}
		require.Equal(b, 500, cursor.PageSize)
		require.Len(b, cursor.Data, 500)
		require.NotEqual(b, "", cursor.Previous)
		require.Equal(b, "", cursor.Next)
	})
}

func getTxQueries(b *testing.B, store *sqlstorage.Store, pageSize, maxNumTxs int) (firstQ, midQ, lastQ *ledger.TransactionsQuery) {
	numTxs := 0
	txQuery := &ledger.TransactionsQuery{
		Filters: ledger.TransactionsQueryFilters{
			Source: ".*:key1:.*:key2:.*",
		},
		PageSize: uint(pageSize),
	}
	firstQ = txQuery
	cursor := api.Cursor[core.ExpandedTransaction]{
		HasMore: true,
	}
	var err error
	for cursor.HasMore {
		if cursor.Next != "" {
			res, decErr := base64.RawURLEncoding.DecodeString(cursor.Next)
			if decErr != nil {
				return
			}

			token := sqlstorage.TxsPaginationToken{}
			if err = json.Unmarshal(res, &token); err != nil {
				return
			}

			txQuery = ledger.NewTransactionsQuery().
				WithAfterTxID(token.AfterTxID).
				WithSourceFilter(token.SourceFilter).
				WithPageSize(token.PageSize)
		}

		cursor, err = store.GetTransactions(context.Background(), *txQuery)
		require.NoError(b, err)
		require.Equal(b, pageSize, cursor.PageSize)
		require.Len(b, cursor.Data, pageSize)
		numTxs += len(cursor.Data)

		if midQ == nil && numTxs > maxNumTxs/2 {
			midQ = txQuery
		}
	}
	lastQ = txQuery
	require.Equal(b, maxNumTxs, numTxs)
	return
}

func testGetTransaction(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2)
	require.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func testTransactions(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3)
	require.NoError(t, err)

	t.Run("Count", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), ledger.TransactionsQuery{})
		require.NoError(t, err)
		// Should get all the transactions
		require.EqualValues(t, 3, count)

		count, err = store.CountTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account: "world",
			},
		})
		require.NoError(t, err)
		// Should get the two first transactions involving the 'world' account.
		require.EqualValues(t, 2, count)

		count, err = store.CountTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account:   "world",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
			},
		})
		require.NoError(t, err)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		require.EqualValues(t, 1, count)

		count, err = store.CountTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Metadata: map[string]string{
					"priority": "high",
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	})

	t.Run("Get", func(t *testing.T) {
		cursor, err := store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			PageSize: 1,
		})
		require.NoError(t, err)
		// Should get only the first transaction.
		require.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			AfterTxID: cursor.Data[0].ID,
			PageSize:  1,
		})
		require.NoError(t, err)
		// Should get only the second transaction.
		require.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account:   "world",
				Reference: "tx1",
			},
			PageSize: 1,
		})
		require.NoError(t, err)
		require.Equal(t, 1, cursor.PageSize)
		// Should get only the first transaction.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account: "users:.*",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Source: "central_bank",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Destination: "users:1",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Destination: "users:.*", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Destination: ".*:1", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Source: ".*bank", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Metadata: map[string]string{
					"priority": "high",
				},
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		require.Len(t, cursor.Data, 1)
	})
}

func testTransactionsQueryAddress(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3, tx4, tx5)
	require.NoError(t, err)

	t.Run("Count", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), ledger.TransactionsQuery{})
		require.NoError(t, err)
		// Should get all the transactions
		require.EqualValues(t, 5, count)

		count, err = store.CountTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account: "users:1",
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 2, count)
	})

	t.Run("Get transactions with address query filter", func(t *testing.T) {
		cursor, err := store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Account: "users:1",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only tx3.
		assert.Len(t, cursor.Data, 2)
		assert.Equal(t, cursor.Data[0].ID, tx5.ID)
		assert.Equal(t, cursor.Data[1].ID, tx3.ID)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Destination: "users:1",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only tx3.
		assert.Len(t, cursor.Data, 1)
		assert.Equal(t, cursor.Data[0].ID, tx3.ID)

		cursor, err = store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Source: "users:1",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only tx3.
		assert.Len(t, cursor.Data, 1)
		assert.Equal(t, cursor.Data[0].ID, tx5.ID)
	})
}

func testGetTransactionsByAccount(t *testing.T, store *sqlstorage.Store) {
	now := time.Now()
	err := store.Commit(context.Background(), core.ExpandedTransaction{
		Transaction: core.Transaction{
			TransactionData: core.TransactionData{
				Postings: core.Postings{
					{
						Source:      "a:b:c",
						Destination: "d:e:f",
						Amount:      core.NewMonetaryInt(10),
						Asset:       "USD",
					},
				},
				Timestamp: now,
			},
			ID: 0,
		},
	})
	require.NoError(t, err)

	txs, err := store.GetTransactions(context.Background(), *ledger.NewTransactionsQuery().WithAccountFilter("a:e:c"))
	require.NoError(t, err)
	require.Empty(t, txs.Data)
}
