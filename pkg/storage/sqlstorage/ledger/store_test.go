package ledger_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestStore(t *testing.T) {
	type testingFunction struct {
		name string
		fn   func(t *testing.T, store storage.LedgerStore)
	}

	for _, tf := range []testingFunction{
		{name: "UpdateTransactionMetadata", fn: testUpdateTransactionMetadata},
		{name: "UpdateAccountMetadata", fn: testUpdateAccountMetadata},
		{name: "GetLastLog", fn: testGetLastLog},
		{name: "GetLogs", fn: testGetLogs},
		{name: "CountAccounts", fn: testCountAccounts},
		{name: "GetAssetsVolumes", fn: testGetAssetsVolumes},
		{name: "GetAccounts", fn: testGetAccounts},
		{name: "Transactions", fn: testTransactions},
		{name: "GetTransaction", fn: testGetTransaction},
		{name: "GetBalances", fn: testGetBalances},
		{name: "GetBalancesAggregated", fn: testGetBalancesAggregated},
		{name: "ComputeAccount", fn: testComputeAccount},
	} {
		t.Run(fmt.Sprintf("postgres/%s", tf.name), func(t *testing.T) {
			done := make(chan struct{})
			app := fx.New(
				ledgertesting.ProvideStorageDriver(t),
				fx.NopLogger,
				fx.Invoke(func(driver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							defer func() {
								close(done)
							}()
							store, _, err := driver.GetLedgerStore(ctx, uuid.NewString(), true)
							if err != nil {
								return err
							}
							defer store.Close(ctx)

							if _, err = store.Initialize(context.Background()); err != nil {
								return err
							}

							tf.fn(t, store)
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

var now = core.Now()
var tx1 = core.ExpandedTransaction{
	Transaction: core.Transaction{
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      core.NewMonetaryInt(100),
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
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
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
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(0),
			},
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
					Amount:      core.NewMonetaryInt(100),
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
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(200),
			},
		},
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(200),
				Output: core.NewMonetaryInt(0),
			},
		},
	},
	PreCommitVolumes: core.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
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
					Amount:      core.NewMonetaryInt(1),
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
				Input:  core.NewMonetaryInt(200),
				Output: core.NewMonetaryInt(0),
			},
		},
		"users:1": {
			"USD": {
				Input:  core.NewMonetaryInt(0),
				Output: core.NewMonetaryInt(0),
			},
		},
	},
	PostCommitVolumes: core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(200),
				Output: core.NewMonetaryInt(1),
			},
		},
		"users:1": {
			"USD": {
				Input:  core.NewMonetaryInt(1),
				Output: core.NewMonetaryInt(0),
			},
		},
	},
}

func testUpdateTransactionMetadata(t *testing.T, store storage.LedgerStore) {
	tx := core.ExpandedTransaction{
		Transaction: core.Transaction{
			ID: 0,
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      core.NewMonetaryInt(100),
						Asset:       "USD",
					},
				},
				Reference: "foo",
				Timestamp: core.Now(),
			},
		},
	}
	err := store.InsertTransactions(context.Background(), tx)
	require.NoError(t, err)

	err = store.UpdateTransactionMetadata(context.Background(), tx.ID, core.Metadata{
		"foo": "bar",
	})
	require.NoError(t, err)

	retrievedTransaction, err := store.GetTransaction(context.Background(), tx.ID)
	require.NoError(t, err)
	require.EqualValues(t, "bar", retrievedTransaction.Metadata["foo"])
}

func testUpdateAccountMetadata(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.EnsureAccountExists(context.Background(), "central_bank"))

	err := store.UpdateAccountMetadata(context.Background(), "central_bank", core.Metadata{
		"foo": "bar",
	})
	require.NoError(t, err)

	account, err := store.GetAccount(context.Background(), "central_bank")
	require.NoError(t, err)
	require.EqualValues(t, "bar", account.Metadata["foo"])
}

func testCountAccounts(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
	require.NoError(t, store.EnsureAccountExists(context.Background(), "central_bank"))

	countAccounts, err := store.CountAccounts(context.Background(), storage.AccountsQuery{})
	require.NoError(t, err)
	require.EqualValues(t, 2, countAccounts) // world + central_bank
}

func testGetAssetsVolumes(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
		"central_bank": {
			"USD": {
				Input:  core.NewMonetaryInt(100),
				Output: core.NewMonetaryInt(0),
			},
		},
	}))

	volumes, err := store.GetAssetsVolumes(context.Background(), "central_bank")
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.EqualValues(t, core.NewMonetaryInt(100), volumes["USD"].Input)
	require.EqualValues(t, core.NewMonetaryInt(0), volumes["USD"].Output)
}

func testGetAccounts(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "world", core.Metadata{
		"foo": json.RawMessage(`"bar"`),
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "bank", core.Metadata{
		"hello": json.RawMessage(`"world"`),
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:1", core.Metadata{
		"hello": json.RawMessage(`"world"`),
	}))
	require.NoError(t, store.UpdateAccountMetadata(context.Background(), "order:2", core.Metadata{
		"number":  json.RawMessage(`3`),
		"boolean": json.RawMessage(`true`),
		"a":       json.RawMessage(`{"super": {"nested": {"key": "hello"}}}`),
	}))

	accounts, err := store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, accounts.PageSize)
	require.Len(t, accounts.Data, 1)

	accounts, err = store.GetAccounts(context.Background(), storage.AccountsQuery{
		PageSize:     1,
		AfterAddress: string(accounts.Data[0].Address),
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

func testTransactions(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.InsertTransactions(context.Background(), tx1, tx2, tx3))

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

func testGetTransaction(t *testing.T, store storage.LedgerStore) {
	require.NoError(t, store.InsertTransactions(context.Background(), tx1, tx2))

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func TestInitializeStore(t *testing.T) {
	driver := ledgertesting.StorageDriver(t)
	defer func(driver storage.Driver, ctx context.Context) {
		require.NoError(t, driver.Close(ctx))
	}(driver, context.Background())

	err := driver.Initialize(context.Background())
	require.NoError(t, err)

	store, _, err := driver.GetLedgerStore(context.Background(), uuid.NewString(), true)
	require.NoError(t, err)

	modified, err := store.Initialize(context.Background())
	require.NoError(t, err)
	require.True(t, modified)

	modified, err = store.Initialize(context.Background())
	require.NoError(t, err)
	require.False(t, modified)
}

func testGetLastLog(t *testing.T, store storage.LedgerStore) {
	logTx := core.NewTransactionLog(tx1.Transaction, nil)
	require.NoError(t, store.AppendLog(context.Background(), &logTx))

	lastLog, err := store.GetLastLog(context.Background())
	require.NoError(t, err)
	require.NotNil(t, lastLog)

	require.Equal(t, tx1.Postings, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx1.Reference, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx1.Timestamp, lastLog.Data.(core.NewTransactionLogPayload).Transaction.Timestamp)
}

func testGetLogs(t *testing.T, store storage.LedgerStore) {
	for _, tx := range []core.ExpandedTransaction{tx1, tx2, tx3} {
		logTx := core.NewTransactionLog(tx.Transaction, nil)
		require.NoError(t, store.AppendLog(context.Background(), &logTx))
	}

	cursor, err := store.GetLogs(context.Background(), storage.NewLogsQuery())
	require.NoError(t, err)
	require.Equal(t, storage.QueryDefaultPageSize, cursor.PageSize)

	require.Equal(t, 3, len(cursor.Data))
	require.Equal(t, uint64(2), cursor.Data[0].ID)
	require.Equal(t, tx3.Postings, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Postings)
	require.Equal(t, tx3.Reference, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Reference)
	require.Equal(t, tx3.Timestamp, cursor.Data[0].Data.(core.NewTransactionLogPayload).Transaction.Timestamp)

	cursor, err = store.GetLogs(context.Background(), &storage.LogsQuery{
		PageSize: 1,
	})
	require.NoError(t, err)
	// Should get only the first log.
	require.Equal(t, 1, cursor.PageSize)
	require.Equal(t, uint64(2), cursor.Data[0].ID)

	cursor, err = store.GetLogs(context.Background(), &storage.LogsQuery{
		AfterID:  cursor.Data[0].ID,
		PageSize: 1,
	})
	require.NoError(t, err)
	// Should get only the second log.
	require.Equal(t, 1, cursor.PageSize)
	require.Equal(t, uint64(1), cursor.Data[0].ID)

	cursor, err = store.GetLogs(context.Background(), &storage.LogsQuery{
		Filters: storage.LogsQueryFilters{
			StartTime: now.Add(-2 * time.Hour),
			EndTime:   now.Add(-1 * time.Hour),
		},
		PageSize: 10,
	})
	require.NoError(t, err)
	require.Equal(t, 10, cursor.PageSize)
	// Should get only the second log, as StartTime is inclusive and EndTime exclusive.
	require.Len(t, cursor.Data, 1)
	require.Equal(t, uint64(1), cursor.Data[0].ID)
}
