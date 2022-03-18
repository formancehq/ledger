package sqlstorage_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func BenchmarkStore(b *testing.B) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	pgServer, err := pgtesting.PostgresServer()
	assert.NoError(b, err)
	defer pgServer.Close()

	type driverConfig struct {
		driver     string
		connString sqlstorage.ConnStringResolver
		flavor     sqlbuilder.Flavor
	}
	var drivers = []driverConfig{
		{
			driver: "sqlite3",
			connString: func(name string) string {
				return sqlstorage.SQLiteFileConnString(path.Join(os.TempDir(), name))
			},
			flavor: sqlbuilder.SQLite,
		},
		{
			driver: "pgx",
			connString: func(name string) string {
				return pgServer.ConnString()
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
				name: "FindTransactions",
				fn:   testBenchmarkFindTransactions,
			},
			{
				name: "LastLog",
				fn:   testBenchmarkLastLog,
			},
			{
				name: "AggregateVolumes",
				fn:   testBenchmarkAggregateVolumes,
			},
			{
				name: "SaveTransactions",
				fn:   testBenchmarkSaveTransactions,
			},
		} {
			b.Run(fmt.Sprintf("%s/%s", driver.driver, tf.name), func(b *testing.B) {
				ledger := uuid.New()

				db, err := sql.Open(driver.driver, driver.connString(ledger))
				assert.NoError(b, err)

				counter := 0
				for {
					err = db.Ping()
					if err != nil {
						if counter < 5 {
							counter++
							<-time.After(time.Second)
							continue
						}
						assert.Fail(b, "timeout waiting database: %s", err)
						return
					}
					break
				}

				store, err := sqlstorage.NewStore(ledger, driver.flavor, db, func(ctx context.Context) error {
					return db.Close()
				})
				assert.NoError(b, err)
				defer store.Close(context.Background())

				_, err = store.Initialize(context.Background())
				assert.NoError(b, err)

				b.ResetTimer()

				tf.fn(b, store)
			})
		}
	}
}

func testBenchmarkFindTransactions(b *testing.B, store *sqlstorage.Store) {
	var log *core.Log
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
			ID: uuid.New(),
		}
		*log = core.NewTransactionLog(log, tx)
		_, err := store.AppendLog(context.Background(), *log)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txs, err := store.FindTransactions(context.Background(), query.Query{
			Limit: 100,
		})
		assert.NoError(b, err)
		if txs.PageSize != 100 {
			b.Errorf("Should have 100 transactions but get %d", txs.PageSize)
		}
	}

}

func testBenchmarkLastLog(b *testing.B, store *sqlstorage.Store) {
	var log *core.Log
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
			ID: uuid.New(),
		}
		*log = core.NewTransactionLog(log, tx)
		_, err := store.AppendLog(context.Background(), *log)
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
	var log *core.Log
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
			ID: uuid.New(),
		}
		*log = core.NewTransactionLog(log, tx)
		_, err := store.AppendLog(context.Background(), *log)
		assert.NoError(b, err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := store.AggregateVolumes(context.Background(), "world")
		assert.NoError(b, err)
	}

}

func testBenchmarkSaveTransactions(b *testing.B, store *sqlstorage.Store) {
	var log *core.Log
	for n := 0; n < b.N; n++ {
		*log = core.NewTransactionLog(log, core.Transaction{
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
			ID: uuid.New(),
		})
		_, err := store.AppendLog(context.Background(), *log)
		assert.NoError(b, err)
	}
}
