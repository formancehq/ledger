package sqlstorage_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestStore(t *testing.T) {
	l := logrus.New()
	if testing.Verbose() {
		l.Level = logrus.DebugLevel
	}
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	type testingFunction struct {
		name string
		fn   func(t *testing.T, store *sqlstorage.Store)
	}

	for _, tf := range []testingFunction{
		{name: "AppendLog", fn: testAppendLog},
		{name: "LastLog", fn: testLastLog},
		{name: "CountAccounts", fn: testCountAccounts},
		{name: "GetAssetsVolumes", fn: testGetAssetsVolumes},
		{name: "GetAccounts", fn: testGetAccounts},
		{name: "Transactions", fn: testTransactions},
		{name: "GetTransaction", fn: testGetTransaction},
		{name: "Mapping", fn: testMapping},
		{name: "TooManyClient", fn: testTooManyClient},
		{name: "GetBalances", fn: testGetBalances},
		{name: "GetBalancesAggregated", fn: testGetBalancesAggregated},
	} {
		t.Run(fmt.Sprintf("%s/%s", ledgertesting.StorageDriverName(), tf.name), func(t *testing.T) {
			done := make(chan struct{})
			app := fx.New(
				ledgertesting.ProvideStorageDriver(),
				fx.Invoke(func(driver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							defer func() {
								close(done)
							}()
							ledger := uuid.New()
							store, _, err := driver.GetStore(ctx, ledger, true)
							if err != nil {
								return err
							}
							defer func(store storage.Store, ctx context.Context) {
								require.NoError(t, store.Close(ctx))
							}(store, context.Background())

							_, err = store.Initialize(context.Background())
							if err != nil {
								return err
							}

							tf.fn(t, store.(*sqlstorage.Store))
							return nil
						},
					})
				}),
			)
			go func() {
				require.NoError(t, app.Start(context.Background()))
			}()
			defer func(app *fx.App, ctx context.Context) {
				require.NoError(t, app.Stop(ctx))
			}(app, context.Background())

			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timeout")
			case <-done:
			}
		})
	}
}

var tx1 = core.Transaction{
	TransactionData: core.TransactionData{
		Postings: []core.Posting{
			{
				Source:      "world",
				Destination: "central_bank",
				Amount:      100,
				Asset:       "USD",
			},
		},
		Reference: "tx1",
		Timestamp: now.Add(-3 * time.Hour),
	},
}
var tx2 = core.Transaction{
	ID: 1,
	TransactionData: core.TransactionData{
		Postings: []core.Posting{
			{
				Source:      "world",
				Destination: "central_bank",
				Amount:      100,
				Asset:       "USD",
			},
		},
		Reference: "tx2",
		Timestamp: now.Add(-2 * time.Hour),
	},
}
var tx3 = core.Transaction{
	ID: 2,
	TransactionData: core.TransactionData{
		Postings: []core.Posting{
			{
				Source:      "central_bank",
				Destination: "users:1",
				Amount:      1,
				Asset:       "USD",
			},
		},
		Reference: "tx3",
		Metadata: core.Metadata{
			"priority": json.RawMessage(`"high"`),
		},
		Timestamp: now.Add(-1 * time.Hour),
	},
}

func testAppendLog(t *testing.T, store *sqlstorage.Store) {
	log := core.NewTransactionLog(nil, core.Transaction{
		ID: 0,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Round(time.Second),
		},
	})
	err := store.AppendLog(context.Background(), log)
	assert.NoError(t, err)

	err = store.AppendLog(context.Background(), log)
	assert.Error(t, err)
	assert.Equal(t, storage.ConstraintFailed, err.(*storage.Error).Code)
}

func testCountAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: 0,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Round(time.Second),
		},
	}
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	countAccounts, err := store.CountAccounts(context.Background(), storage.AccountsQuery{})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, countAccounts) // world + central_bank
}

func testGetAssetsVolumes(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Timestamp: time.Now().Round(time.Second),
		},
	}
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	volumes, err := store.GetAssetsVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, volumes, 1)
	assert.EqualValues(t, 100, volumes["USD"].Input)
	assert.EqualValues(t, 0, volumes["USD"].Output)
}

func testGetAccounts(t *testing.T, store *sqlstorage.Store) {
	account1 := core.NewSetMetadataLog(nil, core.SetMetadata{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "world",
		Metadata: core.Metadata{
			"foo": json.RawMessage(`"bar"`),
		},
	})
	account2 := core.NewSetMetadataLog(&account1, core.SetMetadata{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata: core.Metadata{
			"hello": json.RawMessage(`"world"`),
		},
	})
	account3 := core.NewSetMetadataLog(&account2, core.SetMetadata{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "order:1",
		Metadata: core.Metadata{
			"hello": json.RawMessage(`"world"`),
		},
	})
	account4 := core.NewSetMetadataLog(&account3, core.SetMetadata{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "order:2",
		Metadata: core.Metadata{
			"number":  json.RawMessage(`3`),
			"boolean": json.RawMessage(`true`),
			"a":       json.RawMessage(`{"super": {"nested": {"key": "hello"}}}`),
		},
	})

	err := store.AppendLog(context.Background(), account1, account2, account3, account4)
	assert.NoError(t, err)

	accounts, err := store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize:     1,
		AfterAddress: accounts.Data[0].Address,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Address: ".*der.*",
		},
	})
	assert.NoError(t, err)
	assert.Len(t, accounts.Data, 2)
	assert.Equal(t, 10, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"foo": "bar",
			},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"number": "3",
			},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"boolean": "true",
			},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"a.super.nested.key": "hello",
			},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, accounts.Data, 1)
}

func testTransactions(t *testing.T, store *sqlstorage.Store) {
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	log3 := core.NewTransactionLog(&log2, tx3)
	err := store.AppendLog(context.Background(), log1, log2, log3)
	assert.NoError(t, err)

	t.Run("Count", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
		assert.NoError(t, err)
		// Should get all the transactions
		assert.EqualValues(t, 3, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account: "world",
			},
		})
		assert.NoError(t, err)
		// Should get the two first transactions involving the 'world' account.
		assert.EqualValues(t, 2, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account:   "world",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
			},
		})
		assert.NoError(t, err)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		assert.EqualValues(t, 1, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Metadata: map[string]string{
					"priority": "high",
				},
			},
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 1, count)
	})

	t.Run("Get", func(t *testing.T) {
		cursor, err := store.GetTransactions(context.Background(), storage.TransactionsQuery{
			PageSize: 1,
		})
		assert.NoError(t, err)
		// Should get only the first transaction.
		assert.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			AfterTxID: cursor.Data[0].ID,
			PageSize:  1,
		})
		assert.NoError(t, err)
		// Should get only the second transaction.
		assert.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account:   "world",
				Reference: "tx1",
			},
			PageSize: 1,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, cursor.PageSize)
		// Should get only the first transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account: "users:.*",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Source: "central_bank",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Destination: "users:1",
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Destination: "users:.*", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Destination: ".*:1", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Source: ".*bank", // Use regex
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		assert.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Metadata: map[string]string{
					"priority": "high",
				},
			},
			PageSize: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		assert.Len(t, cursor.Data, 1)
	})
}

func testMapping(t *testing.T, store *sqlstorage.Store) {
	m := core.Mapping{
		Contracts: []core.Contract{
			{
				Expr: &core.ExprGt{
					Op1: core.VariableExpr{Name: "balance"},
					Op2: core.ConstantExpr{Value: float64(0)},
				},
				Account: "orders:*",
			},
		},
	}
	err := store.SaveMapping(context.Background(), m)
	assert.NoError(t, err)

	mapping, err := store.LoadMapping(context.Background())
	assert.NoError(t, err)
	assert.Len(t, mapping.Contracts, 1)
	assert.EqualValues(t, m.Contracts[0], mapping.Contracts[0])

	m2 := core.Mapping{
		Contracts: []core.Contract{},
	}
	err = store.SaveMapping(context.Background(), m2)
	assert.NoError(t, err)

	mapping, err = store.LoadMapping(context.Background())
	assert.NoError(t, err)
	assert.Len(t, mapping.Contracts, 0)
}

func testGetTransaction(t *testing.T, store *sqlstorage.Store) {
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	err := store.AppendLog(context.Background(), log1, log2)
	assert.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tx1.Postings, tx.Postings)
	assert.Equal(t, tx1.Reference, tx.Reference)
	assert.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func testTooManyClient(t *testing.T, store *sqlstorage.Store) {
	// Use of external server, ignore this test
	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" ||
		ledgertesting.StorageDriverName() != "postgres" {
		t.SkipNow()
	}

	for i := 0; i < pgtesting.MaxConnections; i++ {
		tx, err := store.Schema().BeginTx(context.Background(), nil)
		assert.NoError(t, err)
		defer func(tx *sql.Tx) {
			require.NoError(t, tx.Rollback())
		}(tx)
	}

	_, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
	assert.Error(t, err)
	assert.IsType(t, new(storage.Error), err)
	assert.Equal(t, storage.TooManyClient, err.(*storage.Error).Code)
}

func TestInitializeStore(t *testing.T) {
	l := logrus.New()
	l.Level = logrus.DebugLevel
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	driver, stopFn, err := ledgertesting.StorageDriver()
	assert.NoError(t, err)
	defer stopFn()
	defer func(driver storage.Driver, ctx context.Context) {
		require.NoError(t, driver.Close(ctx))
	}(driver, context.Background())

	err = driver.Initialize(context.Background())
	assert.NoError(t, err)

	store, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	assert.NoError(t, err)

	modified, err := store.Initialize(context.Background())
	assert.NoError(t, err)
	assert.True(t, modified)

	modified, err = store.Initialize(context.Background())
	assert.NoError(t, err)
	assert.False(t, modified)
}

func testLastLog(t *testing.T, store *sqlstorage.Store) {
	log := core.NewTransactionLog(nil, tx1)
	err := store.AppendLog(context.Background(), log)
	assert.NoError(t, err)

	lastLog, err := store.LastLog(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastLog)

	assert.Equal(t, tx1.Postings, lastLog.Data.(core.Transaction).Postings)
	assert.Equal(t, tx1.Reference, lastLog.Data.(core.Transaction).Reference)
	assert.Equal(t, tx1.Timestamp, lastLog.Data.(core.Transaction).Timestamp)
}
