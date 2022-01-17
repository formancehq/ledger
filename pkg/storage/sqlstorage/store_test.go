package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
)

func TestStore(t *testing.T) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	pgServer, err := ledgertesting.PostgresServer()
	assert.NoError(t, err)
	defer pgServer.Close()

	type driverConfig struct {
		driver     string
		connString ConnStringResolver
		flavor     sqlbuilder.Flavor
	}
	var drivers = []driverConfig{
		{
			driver: "sqlite3",
			connString: func(name string) string {
				return SQLiteFileConnString(path.Join(os.TempDir(), name))
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
		fn   func(t *testing.T, store storage.Store)
	}

	for _, driver := range drivers {
		for _, tf := range []testingFunction{
			{
				name: "SaveTransactions",
				fn:   testSaveTransaction,
			},
			{
				name: "DuplicatedTransaction",
				fn:   testDuplicatedTransaction,
			},
			{
				name: "SaveMeta",
				fn:   testSaveMeta,
			},
			{
				name: "LastTransaction",
				fn:   testLastTransaction,
			},
			{
				name: "LastMetaID",
				fn:   testLastMetaID,
			},
			{
				name: "CountAccounts",
				fn:   testCountAccounts,
			},
			{
				name: "AggregateBalances",
				fn:   testAggregateBalances,
			},
			{
				name: "AggregateVolumes",
				fn:   testAggregateVolumes,
			},
			{
				name: "CountMeta",
				fn:   testCountMeta,
			},
			{
				name: "FindAccounts",
				fn:   testFindAccounts,
			},
			{
				name: "CountTransactions",
				fn:   testCountTransactions,
			},
			{
				name: "FindTransactions",
				fn:   testFindTransactions,
			},
			{
				name: "GetMeta",
				fn:   testGetMeta,
			},
			{
				name: "GetTransaction",
				fn:   testGetTransaction,
			},
			{
				name: "Contracts",
				fn:   testContracts,
			},
		} {
			t.Run(fmt.Sprintf("%s/%s", driver.driver, tf.name), func(t *testing.T) {
				ledger := uuid.New()

				db, err := sql.Open(driver.driver, driver.connString(ledger))
				assert.NoError(t, err)

				counter := 0
				for {
					err = db.Ping()
					if err != nil {
						if counter < 5 {
							counter++
							<-time.After(time.Second)
							continue
						}
						assert.Fail(t, "timeout waiting database: %s", err)
						return
					}
					break
				}

				store, err := NewStore(ledger, driver.flavor, db, func(ctx context.Context) error {

					return db.Close()
				})
				assert.NoError(t, err)
				defer store.Close(context.Background())

				err = store.Initialize(context.Background())
				assert.NoError(t, err)

				tf.fn(t, store)
			})
		}
	}
}

func testSaveTransaction(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)
}

func testDuplicatedTransaction(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	err = store.SaveTransactions(context.Background(), txs)
	assert.Error(t, err)
	assert.IsType(t, &storage.Error{}, err)
	assert.Equal(t, storage.ConstraintFailed, err.(*storage.Error).Code)
}

func testSaveMeta(t *testing.T, store storage.Store) {
	err := store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)
}

func testGetMeta(t *testing.T, store storage.Store) {
	var (
		firstname = "\"John\""
		lastname  = "\"Doe\""
	)
	err := store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339), "transaction", "1", "firstname", firstname)
	assert.NoError(t, err)

	err = store.SaveMeta(context.Background(), 2, time.Now().Format(time.RFC3339), "transaction", "1", "lastname", lastname)
	assert.NoError(t, err)

	meta, err := store.GetMeta(context.TODO(), "transaction", "1")
	assert.NoError(t, err)

	assert.Equal(t, core.Metadata{
		"firstname": json.RawMessage(firstname),
		"lastname":  json.RawMessage(lastname),
	}, meta)
}

func testLastTransaction(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	lastTx, err := store.LastTransaction(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastTx)
	assert.Equal(t, core.Transaction{
		ID:        txs[0].ID,
		Postings:  txs[0].Postings,
		Timestamp: txs[0].Timestamp,
		Metadata:  map[string]json.RawMessage{},
	}, *lastTx)

}

func testLastMetaID(t *testing.T, store storage.Store) {
	err := store.SaveMeta(context.Background(), 0, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)

	lastMetaId, err := store.LastMetaID(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 0, lastMetaId)
}

func testCountAccounts(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	countAccounts, err := store.CountAccounts(context.Background())
	assert.EqualValues(t, 2, countAccounts) // world + central_bank

}

func testAggregateBalances(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	balances, err := store.AggregateBalances(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, balances, 1)
	assert.EqualValues(t, 100, balances["USD"])
}

func testAggregateVolumes(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	volumes, err := store.AggregateVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, volumes, 1)
	assert.Len(t, volumes["USD"], 1)
	assert.EqualValues(t, 100, volumes["USD"]["input"])
}

func testFindAccounts(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	accounts, err := store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, accounts.Total)
	assert.True(t, accounts.HasMore)
	assert.Equal(t, 1, accounts.PageSize)

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
		After: accounts.Data.([]core.Account)[0].Address,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, accounts.Total)
	assert.False(t, accounts.HasMore)
	assert.Equal(t, 1, accounts.PageSize)
}

func testCountMeta(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Metadata: map[string]json.RawMessage{
				"lastname": json.RawMessage(`"XXX"`),
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	err = store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)

	countMeta, err := store.CountMeta(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 2, countMeta)
}

func testCountTransactions(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Metadata: map[string]json.RawMessage{
				"lastname": json.RawMessage(`"XXX"`),
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	countTransactions, err := store.CountTransactions(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 1, countTransactions)
}

func testFindTransactions(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 0,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
			Reference: "tx1",
		},
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
			Reference: "tx2",
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	cursor, err := store.FindTransactions(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.True(t, cursor.HasMore)

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		After: fmt.Sprint(cursor.Data.([]core.Transaction)[0].ID),
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.False(t, cursor.HasMore)

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		Params: map[string]interface{}{
			"account":   "world",
			"reference": "tx1",
		},
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.False(t, cursor.HasMore)

}

func testContracts(t *testing.T, store storage.Store) {
	contract := core.Contract{
		ID: "1",
		Expr: &core.ExprGt{
			Op1: core.VariableExpr{Name: "balance"},
			Op2: core.ConstantExpr{Value: float64(0)},
		},
		Account: "orders:*",
	}
	err := store.SaveContract(context.Background(), contract)
	assert.NoError(t, err)

	contracts, err := store.FindContracts(context.Background())
	assert.NoError(t, err)
	assert.Len(t, contracts, 1)
	assert.EqualValues(t, contract, contracts[0])

	err = store.DeleteContract(context.Background(), contract.ID)
	assert.NoError(t, err)

	contracts, err = store.FindContracts(context.Background())
	assert.NoError(t, err)
	assert.Len(t, contracts, 0)
}

func testGetTransaction(t *testing.T, store storage.Store) {
	txs := []core.Transaction{
		{
			ID: 0,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
			Reference: "tx1",
			Metadata:  core.Metadata{},
		},
		{
			ID: 1,
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
			Reference: "tx2",
			Metadata:  core.Metadata{},
		},
	}
	err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), "1")
	assert.NoError(t, err)
	assert.Equal(t, txs[1], tx)

}
