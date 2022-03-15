package sqlstorage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"os"
	"testing"
	"time"
)

func TestStore(t *testing.T) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	type testCase struct {
		name       string
		onlyDriver string
		fn         func(t *testing.T, store *sqlstorage.Store)
	}

	for _, tf := range []testCase{
		{
			name:       "TooManyClient",
			fn:         testTooManyClient,
			onlyDriver: "pgx",
		},
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
			name: "Mapping",
			fn:   testMapping,
		},
	} {
		if tf.onlyDriver != "" && tf.onlyDriver != ledgertesting.StorageDriverName() {
			continue
		}
		t.Run(fmt.Sprintf("%s/%s", ledgertesting.StorageDriverName(), tf.name), func(t *testing.T) {

			done := make(chan struct{})
			app := fx.New(
				ledgertesting.StorageModule(),
				fx.Provide(storage.NewDefaultFactory),
				fx.Invoke(func(storageFactory storage.Factory, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							defer func() {
								close(done)
							}()
							ledger := uuid.New()
							store, err := storageFactory.GetStore(ledger)
							assert.NoError(t, err)

							assert.NoError(t, err)
							defer store.Close(context.Background())

							_, err = store.Initialize(context.Background())
							assert.NoError(t, err)

							tf.fn(t, store.(*sqlstorage.Store))
							return nil
						},
					})
				}),
			)
			go app.Start(context.Background())
			defer app.Stop(context.Background())

			select {
			case <-time.After(5 * time.Second):
			case <-done:
			}
		})
	}
}

func testSaveTransaction(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)
}

func testDuplicatedTransaction(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
			ID: 1,
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{},
				},
				Reference: "foo",
			},
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	txs[0].ID = 2
	ret, err := store.SaveTransactions(context.Background(), txs)
	assert.Error(t, err)
	assert.Equal(t, storage.ErrAborted, err)
	assert.Len(t, ret, 1)
	assert.IsType(t, &storage.Error{}, ret[0])
	assert.Equal(t, storage.ConstraintFailed, ret[0].(*storage.Error).Code)
}

func testSaveMeta(t *testing.T, store *sqlstorage.Store) {
	err := store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)
}

func testGetMeta(t *testing.T, store *sqlstorage.Store) {
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

func testLastTransaction(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	lastTx, err := store.LastTransaction(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastTx)
	assert.Equal(t, core.Transaction{
		TransactionData: core.TransactionData{
			Postings: txs[0].Postings,
			Metadata: map[string]json.RawMessage{},
		},
		ID:        txs[0].ID,
		Timestamp: txs[0].Timestamp,
	}, *lastTx)

}

func testLastMetaID(t *testing.T, store *sqlstorage.Store) {
	err := store.SaveMeta(context.Background(), 0, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)

	lastMetaId, err := store.LastMetaID(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 0, lastMetaId)
}

func testCountAccounts(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	countAccounts, err := store.CountAccounts(context.Background())
	assert.EqualValues(t, 2, countAccounts) // world + central_bank

}

func testAggregateBalances(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	balances, err := store.AggregateBalances(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, balances, 1)
	assert.EqualValues(t, 100, balances["USD"])
}

func testAggregateVolumes(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	volumes, err := store.AggregateVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, volumes, 1)
	assert.Len(t, volumes["USD"], 1)
	assert.EqualValues(t, 100, volumes["USD"]["input"])
}

func testFindAccounts(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
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

func testCountMeta(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
				Metadata: map[string]json.RawMessage{
					"lastname": json.RawMessage(`"XXX"`),
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	err = store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)

	countMeta, err := store.CountMeta(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 2, countMeta)
}

func testCountTransactions(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
				Metadata: map[string]json.RawMessage{
					"lastname": json.RawMessage(`"XXX"`),
				},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	countTransactions, err := store.CountTransactions(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 1, countTransactions)
}

func testFindTransactions(t *testing.T, store *sqlstorage.Store) {
	txs := []core.Transaction{
		{
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
				Reference: "tx1",
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
		{
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
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
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
	txs := []core.Transaction{
		{
			ID:        0,
			Timestamp: time.Now().Format(time.RFC3339),
			TransactionData: core.TransactionData{
				Reference: "tx1",
				Metadata:  core.Metadata{},
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      100,
						Asset:       "USD",
					},
				},
			},
		},
		{
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
				Metadata:  core.Metadata{},
			},
			Timestamp: time.Now().Format(time.RFC3339),
		},
	}
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), "1")
	assert.NoError(t, err)
	assert.Equal(t, txs[1], tx)
}

func testTooManyClient(t *testing.T, store *sqlstorage.Store) {

	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" { // Use of external server, ignore this test
		return
	}

	for i := 0; i < ledgertesting.MaxConnections; i++ {
		tx, err := store.DB().BeginTx(context.Background(), nil)
		assert.NoError(t, err)
		defer tx.Rollback()
	}
	_, err := store.CountTransactions(context.Background())
	assert.Error(t, err)
	assert.IsType(t, new(storage.Error), err)
	assert.Equal(t, storage.TooManyClient, err.(*storage.Error).Code)
}

func TestInitializeStore(t *testing.T) {

	driver, stopFn, err := ledgertesting.Driver()
	assert.NoError(t, err)
	defer stopFn()
	defer driver.Close(context.Background())

	err = driver.Initialize(context.Background())
	assert.NoError(t, err)

	store, err := driver.NewStore(uuid.New())
	assert.NoError(t, err)

	modified, err := store.Initialize(context.Background())
	assert.NoError(t, err)
	assert.True(t, modified)

	modified, err = store.Initialize(context.Background())
	assert.NoError(t, err)
	assert.False(t, modified)
}
