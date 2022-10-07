package sqlstorage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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

					getTransactions(b, store)
					return nil
				},
			})
		}))

	require.NoError(b, app.Start(context.Background()))
	defer func(app *fx.App, ctx context.Context) {
		require.NoError(b, app.Stop(ctx))
	}(app, context.Background())
}

func getTransactions(b *testing.B, store *sqlstorage.Store) {
	pages := 120
	pageSize := 500
	id := uint64(0)
	for i := 0; i < pages; i++ {
		for j := 0; j < pageSize; j++ {
			acc := uuid.NewString() + ":main:" + uuid.NewString() + ":" + uuid.NewString()
			tx := core.ExpandedTransaction{
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
			}
			err := store.Commit(context.Background(), tx)
			require.NoError(b, err)
			id++
		}
	}

	nb, err := store.CountTransactions(context.Background(), ledger.TransactionsQuery{})
	require.NoError(b, err)
	require.Equal(b, uint64(pages*pageSize), nb)

	b.ResetTimer()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		cursor, err := store.GetTransactions(context.Background(), ledger.TransactionsQuery{
			Filters: ledger.TransactionsQueryFilters{
				Source: ".*:main:.*:.*",
			},
			PageSize: uint(pageSize),
		})
		require.NoError(b, err)
		require.Equal(b, pageSize, cursor.PageSize)
		require.Len(b, cursor.Data, pageSize)
	}
}
