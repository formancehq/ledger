package sqlstorage_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func BenchmarkStore_GetTransactions(b *testing.B) {
	b.StopTimer()
	l := logrus.New()
	if testing.Verbose() {
		l.Level = logrus.DebugLevel
	}
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	app := fx.New(
		fx.NopLogger,
		ledgertesting.ProvideStorageDriver(),
		fx.Invoke(func(driver *sqlstorage.Driver, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					ledgerName := uuid.NewString()
					store, _, err := driver.GetLedgerStore(ctx, ledgerName, true)
					if err != nil {
						return err
					}
					defer func(store ledger.Store, ctx context.Context) {
						require.NoError(b, store.Close(ctx))
					}(store, context.Background())

					_, err = store.Initialize(context.Background())
					if err != nil {
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

	b.ResetTimer()
	b.StartTimer()

	b.Run(fmt.Sprintf("only first page of size %d out of %d txs", 1, numTxs),
		func(b *testing.B) { getFirstPage(b, store, 1) })
	b.Run(fmt.Sprintf("only first page of size %d out of %d txs", 10, numTxs),
		func(b *testing.B) { getFirstPage(b, store, 10) })
	b.Run(fmt.Sprintf("only first page of size %d out of %d txs", 50, numTxs),
		func(b *testing.B) { getFirstPage(b, store, 50) })
	b.Run(fmt.Sprintf("only first page of size %d out of %d txs", 500, numTxs),
		func(b *testing.B) { getFirstPage(b, store, 500) })

	b.Run(fmt.Sprintf("all pages of size %d out of %d txs", 1, numTxs),
		func(b *testing.B) { getAllPages(b, store, 1, maxPages*maxPageSize) })
	b.Run(fmt.Sprintf("all pages of size %d out of %d txs", 10, numTxs),
		func(b *testing.B) { getAllPages(b, store, 10, maxPages*maxPageSize) })
	b.Run(fmt.Sprintf("all pages of size %d out of %d txs", 50, numTxs),
		func(b *testing.B) { getAllPages(b, store, 50, maxPages*maxPageSize) })
	b.Run(fmt.Sprintf("all pages of size %d out of %d txs", 500, numTxs),
		func(b *testing.B) { getAllPages(b, store, 500, maxPages*maxPageSize) })
}

func getFirstPage(b *testing.B, store *sqlstorage.Store, pageSize int) {
	for n := 0; n < b.N; n++ {
		cursor, err := store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Source: ".*:key1:.*:key2:.*",
			},
			PageSize: uint(pageSize),
		})
		require.NoError(b, err)
		require.Equal(b, pageSize, cursor.PageSize)
		require.Len(b, cursor.Data, pageSize)
	}
}

func getAllPages(b *testing.B, store *sqlstorage.Store, pageSize, maxNumTxs int) {
	for n := 0; n < b.N; n++ {
		numTxs := 0
		var txQuery *ledger.TransactionsQuery
		cursor := sharedapi.Cursor[core.ExpandedTransaction]{
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
			} else {
				txQuery = &ledger.TransactionsQuery{
					Filters: ledger.TransactionsQueryFilters{
						Source: ".*:key1:.*:key2:.*",
					},
					PageSize: uint(pageSize),
				}
			}

			cursor, err = store.GetTransactions(context.Background(), *txQuery)
			require.NoError(b, err)
			require.Equal(b, pageSize, cursor.PageSize)
			require.Len(b, cursor.Data, pageSize)
			numTxs += len(cursor.Data)
		}

		require.Equal(b, maxNumTxs, numTxs)
	}
}
