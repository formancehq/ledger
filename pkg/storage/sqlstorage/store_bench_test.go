package sqlstorage_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkStore(b *testing.B) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	pgServer, err := pgtesting.PostgresServer()
	assert.NoError(b, err)
	defer func(pgServer *pgtesting.PGServer) {
		require.NoError(b, pgServer.Close())
	}(pgServer)

	type driverConfig struct {
		driver    string
		dbFactory func() (sqlstorage.DB, error)
		flavor    sqlbuilder.Flavor
	}
	var drivers = []driverConfig{
		{
			driver: "sqlite3",
			dbFactory: func() (sqlstorage.DB, error) {
				return sqlstorage.NewSQLiteDB(os.TempDir(), uuid.New()), nil
			},
			flavor: sqlbuilder.SQLite,
		},
		{
			driver: "pgx",
			dbFactory: func() (sqlstorage.DB, error) {
				db, err := sqlstorage.OpenSQLDB(sqlstorage.PostgreSQL, pgServer.ConnString())
				if err != nil {
					return nil, err
				}
				return sqlstorage.NewPostgresDB(db), nil
			},
			flavor: sqlbuilder.PostgreSQL,
		},
	}

	type testingFunction struct {
		name string
		fn   func(b *testing.B, store *sqlstorage.Store)
	}

	for _, driver := range drivers {
		for _, tf := range []testingFunction{
			{
				name: "GetTransactions",
				fn:   testBenchmarkGetTransactions,
			},
			{
				name: "LastLog",
				fn:   testBenchmarkLastLog,
			},
			{
				name: "GetAssetsVolumes",
				fn:   testBenchmarkAggregateVolumes,
			},
			{
				name: "SaveTransactions",
				fn:   testBenchmarkSaveTransactions,
			},
		} {
			b.Run(fmt.Sprintf("%s/%s", driver.driver, tf.name), func(b *testing.B) {
				db, err := driver.dbFactory()
				if !assert.NoError(b, err) {
					return
				}

				schema, err := db.Schema(context.Background(), uuid.New())
				if !assert.NoError(b, err) {
					return
				}

				store := sqlstorage.NewStore(schema, func(ctx context.Context) error {
					return db.Close(context.Background())
				})

				defer func(store *sqlstorage.Store, ctx context.Context) {
					require.NoError(b, store.Close(ctx))
				}(store, context.Background())

				_, err = store.Initialize(context.Background())
				assert.NoError(b, err)

				b.ResetTimer()

				tf.fn(b, store)
			})
		}
	}
}

func testBenchmarkGetTransactions(b *testing.B, store *sqlstorage.Store) {
	for i := 0; i < 1000; i++ {
		tx := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i),
						Asset:       "USD",
						Amount:      100,
					},
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i+1),
						Asset:       "USD",
						Amount:      100,
					},
				},
			},
			ID: uint64(i),
		}
		err := store.Commit(context.Background(), tx)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txs, err := store.GetTransactions(context.Background(), storage.TransactionsQuery{
			PageSize: 100,
		})
		assert.NoError(b, err)
		if txs.PageSize != 100 {
			b.Errorf("Should have 100 transactions but get %d", txs.PageSize)
		}
	}

}

func testBenchmarkLastLog(b *testing.B, store *sqlstorage.Store) {
	count := 1000
	for i := 0; i < count; i++ {
		tx := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i),
						Asset:       "USD",
						Amount:      100,
					},
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i+1),
						Asset:       "USD",
						Amount:      100,
					},
				},
			},
			ID: uint64(i),
		}
		err := store.Commit(context.Background(), tx)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		lastLog, err := store.LastLog(context.Background())
		assert.NoError(b, err)
		assert.Equal(b, int64(count-1), lastLog.ID)
	}

}

func testBenchmarkAggregateVolumes(b *testing.B, store *sqlstorage.Store) {
	count := 1000
	for i := 0; i < count; i++ {
		tx := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i),
						Asset:       "USD",
						Amount:      100,
					},
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", i+1),
						Asset:       "USD",
						Amount:      100,
					},
					{
						Source:      fmt.Sprintf("player%d", i),
						Destination: fmt.Sprintf("player%d", i+1),
						Asset:       "USD",
						Amount:      50,
					},
				},
			},
			ID: uint64(i),
		}
		err := store.Commit(context.Background(), tx)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := store.GetAssetsVolumes(context.Background(), "world")
		assert.NoError(b, err)
	}

}

func testBenchmarkSaveTransactions(b *testing.B, store *sqlstorage.Store) {
	for n := 0; n < b.N; n++ {
		tx := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: fmt.Sprintf("player%d", n),
						Asset:       "USD",
						Amount:      100,
					},
				},
			},
			ID: uint64(n),
		}
		err := store.Commit(context.Background(), tx)
		assert.NoError(b, err)
	}
}
