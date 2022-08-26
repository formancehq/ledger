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
		{name: "Commit", fn: testCommit},
		{name: "UpdateTransactionMetadata", fn: testUpdateTransactionMetadata},
		{name: "UpdateAccountMetadata", fn: testUpdateAccountMetadata},
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
				if err := app.Start(context.Background()); err != nil {
					sharedlogging.Errorf("START ERR: %s", err)
					t.Errorf("START ERR: %s", err)
				}
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

var now = time.Now().UTC().Truncate(time.Second)
var tx1 = core.ExpandedTransaction{
	Transaction: core.Transaction{
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
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Output: 100,
			},
		},
		"central_bank": {
			"USD": {
				Input: 100,
			},
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {},
		},
		"central_bank": {
			"USD": {},
		},
	},
}
var tx2 = core.ExpandedTransaction{
	Transaction: core.Transaction{
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
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Output: 200,
			},
		},
		"central_bank": {
			"USD": {
				Input: 200,
			},
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Output: 100,
			},
		},
		"central_bank": {
			"USD": {
				Input: 100,
			},
		},
	},
}
var tx3 = core.ExpandedTransaction{
	Transaction: core.Transaction{
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
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input: 200,
			},
		},
		"users:1": {
			"USD": {},
		},
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  200,
				Output: 1,
			},
		},
		"users:1": {
			"USD": {
				Input: 1,
			},
		},
	},
}

func testCommit(t *testing.T, store *sqlstorage.Store) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
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
				Reference: "foo",
				Timestamp: time.Now().Round(time.Second),
			},
		},
	}
	err := store.Commit(context.Background(), tx)
	require.NoError(t, err)

	err = store.Commit(context.Background(), tx)
	require.Error(t, err)
	require.True(t, storage.IsErrorCode(err, storage.ConstraintFailed))

	logs, err := store.Logs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
}

func testUpdateTransactionMetadata(t *testing.T, store *sqlstorage.Store) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
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
				Reference: "foo",
				Timestamp: time.Now().Round(time.Second),
			},
		},
	}
	err := store.Commit(context.Background(), tx)
	require.NoError(t, err)

	err = store.UpdateTransactionMetadata(context.Background(), tx.ID, core.Metadata{
		"foo": "bar",
	}, time.Now())
	require.NoError(t, err)

	retrievedTransaction, err := store.GetTransaction(context.Background(), tx.ID)
	require.NoError(t, err)
	require.EqualValues(t, "bar", retrievedTransaction.Metadata["foo"])

	logs, err := store.Logs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 2)
}

func testUpdateAccountMetadata(t *testing.T, store *sqlstorage.Store) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
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
				Reference: "foo",
				Timestamp: time.Now().Round(time.Second),
			},
		},
	}
	err := store.Commit(context.Background(), tx)
	require.NoError(t, err)

	err = store.UpdateAccountMetadata(context.Background(), "central_bank", core.Metadata{
		"foo": "bar",
	}, time.Now())
	require.NoError(t, err)

	account, err := store.GetAccount(context.Background(), "central_bank")
	require.NoError(t, err)
	require.EqualValues(t, "bar", account.Metadata["foo"])

	logs, err := store.Logs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 2)
}

func testCountAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
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
		},
	}
	err := store.Commit(context.Background(), tx)
	require.NoError(t, err)

	countAccounts, err := store.CountAccounts(context.Background(), storage.AccountsQuery{})
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}

func testGetAssetsVolumes(t *testing.T, store *sqlstorage.Store) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
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
		},
		PostCommitVolumes: core.AccountsAssetsVolumes{
			"central_bank": core.AssetsVolumes{
				"USD": {
					Input: 100,
				},
			},
		},
		PreCommitVolumes: core.AccountsAssetsVolumes{
			"central_bank": core.AssetsVolumes{
				"USD": {
					Input: 100,
				},
			},
		},
	}
	err := store.Commit(context.Background(), tx)
	require.NoError(t, err)

	volumes, err := store.GetAssetsVolumes(context.Background(), "central_bank")
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.EqualValues(t, 100, volumes["USD"].Input)
	require.EqualValues(t, 0, volumes["USD"].Output)
}

func testGetAccounts(t *testing.T, store *sqlstorage.Store) {
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "world", core.Metadata{
		"foo": json.RawMessage(`"bar"`),
	}, now))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "bank", core.Metadata{
		"hello": json.RawMessage(`"world"`),
	}, now))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:1", core.Metadata{
		"hello": json.RawMessage(`"world"`),
	}, now))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:2", core.Metadata{
		"number":  json.RawMessage(`3`),
		"boolean": json.RawMessage(`true`),
		"a":       json.RawMessage(`{"super": {"nested": {"key": "hello"}}}`),
	}, now))

	accounts, err := store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, accounts.PageSize)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize:     1,
		AfterAddress: accounts.Data[0].Address,
	})
	require.NoError(t, err)
	require.Equal(t, 1, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Address: ".*der.*",
		},
	})
	require.NoError(t, err)
	require.Len(t, accounts.Data, 2)
	require.Equal(t, 10, accounts.PageSize)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"foo": "bar",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"number": "3",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"boolean": "true",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 10,
		Filters: storage.AccountsQueryFilters{
			Metadata: map[string]string{
				"a.super.nested.key": "hello",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, accounts.Data, 1)
}

func testTransactions(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1, tx2, tx3)
	require.NoError(t, err)

	t.Run("Count", func(t *testing.T) {
		count, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
		require.NoError(t, err)
		// Should get all the transactions
		require.EqualValues(t, 3, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account: "world",
			},
		})
		require.NoError(t, err)
		// Should get the two first transactions involving the 'world' account.
		require.EqualValues(t, 2, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account:   "world",
				StartTime: now.Add(-2 * time.Hour),
				EndTime:   now.Add(-1 * time.Hour),
			},
		})
		require.NoError(t, err)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		require.EqualValues(t, 1, count)

		count, err = store.CountTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Metadata: map[string]string{
					"priority": "high",
				},
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, count)
	})

	t.Run("Get", func(t *testing.T) {
		cursor, err := store.GetTransactions(context.Background(), storage.TransactionsQuery{
			PageSize: 1,
		})
		require.NoError(t, err)
		// Should get only the first transaction.
		require.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			AfterTxID: cursor.Data[0].ID,
			PageSize:  1,
		})
		require.NoError(t, err)
		// Should get only the second transaction.
		require.Equal(t, 1, cursor.PageSize)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account:   "world",
				Reference: "tx1",
			},
			PageSize: 1,
		})
		require.NoError(t, err)
		require.Equal(t, 1, cursor.PageSize)
		// Should get only the first transaction.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Account: "users:.*",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Source: "central_bank",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
				Destination: "users:1",
			},
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only the third transaction.
		require.Len(t, cursor.Data, 1)

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
		require.NoError(t, err)
		require.Equal(t, 10, cursor.PageSize)
		// Should get only tx2, as StartTime is inclusive and EndTime exclusive.
		require.Len(t, cursor.Data, 1)

		cursor, err = store.GetTransactions(context.Background(), storage.TransactionsQuery{
			Filters: storage.TransactionsQueryFilters{
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
	require.NoError(t, err)

	mapping, err := store.LoadMapping(context.Background())
	require.NoError(t, err)
	require.Len(t, mapping.Contracts, 1)
	require.EqualValues(t, m.Contracts[0], mapping.Contracts[0])

	m2 := core.Mapping{
		Contracts: []core.Contract{},
	}
	err = store.SaveMapping(context.Background(), m2)
	require.NoError(t, err)

	mapping, err = store.LoadMapping(context.Background())
	require.NoError(t, err)
	require.Len(t, mapping.Contracts, 0)
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

func testTooManyClient(t *testing.T, store *sqlstorage.Store) {
	// Use of external server, ignore this test
	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" ||
		ledgertesting.StorageDriverName() != "postgres" {
		t.SkipNow()
	}

	for i := 0; i < pgtesting.MaxConnections; i++ {
		tx, err := store.Schema().BeginTx(context.Background(), nil)
		require.NoError(t, err)
		defer func(tx *sql.Tx) {
			require.NoError(t, tx.Rollback())
		}(tx)
	}

	_, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
	require.Error(t, err)
	require.IsType(t, new(storage.Error), err)
	require.Equal(t, storage.TooManyClient, err.(*storage.Error).Code)
}

func TestInitializeStore(t *testing.T) {
	l := logrus.New()
	l.Level = logrus.DebugLevel
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	driver, stopFn, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer stopFn()
	defer func(driver storage.Driver, ctx context.Context) {
		require.NoError(t, driver.Close(ctx))
	}(driver, context.Background())

	err = driver.Initialize(context.Background())
	require.NoError(t, err)

	store, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	modified, err := store.Initialize(context.Background())
	require.NoError(t, err)
	require.True(t, modified)

	modified, err = store.Initialize(context.Background())
	require.NoError(t, err)
	require.False(t, modified)
}

func testLastLog(t *testing.T, store *sqlstorage.Store) {
	err := store.Commit(context.Background(), tx1)
	require.NoError(t, err)

	lastLog, err := store.LastLog(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lastLog)

	require.Equal(t, tx1.Postings, lastLog.Data.(core.Transaction).Postings)
	require.Equal(t, tx1.Reference, lastLog.Data.(core.Transaction).Reference)
	require.Equal(t, tx1.Timestamp, lastLog.Data.(core.Transaction).Timestamp)
}
