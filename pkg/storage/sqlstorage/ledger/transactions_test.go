package ledger_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func BenchmarkStore_GetTransactions(b *testing.B) {
	b.StopTimer()

	app := fx.New(
		fx.NopLogger,
		ledgertesting.ProvideStorageDriver(b),
		fx.Invoke(func(driver *sqlstorage.Driver, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					ledgerName := uuid.NewString()
					var store *ledgerstore.Store
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
		}))

	require.NoError(b, app.Start(context.Background()))
	defer func(app *fx.App, ctx context.Context) {
		require.NoError(b, app.Stop(ctx))
	}(app, context.Background())
}

func benchGetTransactions(b *testing.B, store *ledgerstore.Store) {
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

func getTxQueries(b *testing.B, store *ledgerstore.Store, pageSize, maxNumTxs int) (firstQ, midQ, lastQ *ledger.TransactionsQuery) {
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

			token := ledgerstore.TxsPaginationToken{}
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
