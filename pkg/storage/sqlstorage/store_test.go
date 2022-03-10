package sqlstorage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/internal/pgtesting"
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

	type testingFunction struct {
		name string
		fn   func(t *testing.T, store *sqlstorage.Store)
	}

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
			name: "LastLog",
			fn:   testLastLog,
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
			name: "GetTransaction",
			fn:   testGetTransaction,
		},
		{
			name: "Mapping",
			fn:   testMapping,
		},
		{
			name: "TooManyClient",
			fn:   testTooManyClient,
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", ledgertesting.StorageDriverName(), tf.name), func(t *testing.T) {

			done := make(chan struct{})
			app := fx.New(
				ledgertesting.StorageModule(),
				fx.Invoke(func(storageFactory storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							defer func() {
								close(done)
							}()
							ledger := uuid.New()
							store, _, err := storageFactory.GetStore(ctx, ledger, true)
							if err != nil {
								return err
							}
							defer store.Close(context.Background())

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
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}))
	assert.NoError(t, err)
}

func testDuplicatedTransaction(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	}
	log1 := core.NewTransactionLog(nil, tx)
	_, err := store.AppendLog(context.Background(), log1)
	assert.NoError(t, err)

	log2 := core.NewTransactionLog(&log1, core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	})
	ret, err := store.AppendLog(context.Background(), log2)
	assert.Error(t, err)
	assert.Equal(t, storage.ErrAborted, err)
	assert.Len(t, ret, 1)
	assert.IsType(t, &storage.Error{}, ret[0])
	assert.Equal(t, storage.ConstraintFailed, ret[0].(*storage.Error).Code)
}

func testCountAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	if !assert.NoError(t, err) {
		return
	}

	countAccounts, err := store.CountAccounts(context.Background())
	if !assert.EqualValues(t, 2, countAccounts) { // world + central_bank
		return
	}
}

func testAggregateBalances(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	balances, err := store.AggregateBalances(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, balances, 1)
	assert.EqualValues(t, 100, balances["USD"])
}

func testAggregateVolumes(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	if !assert.NoError(t, err) {
		return
	}

	volumes, err := store.AggregateVolumes(context.Background(), "central_bank")
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, volumes, 1) {
		return
	}
	if !assert.Len(t, volumes["USD"], 2) {
		return
	}
	if !assert.EqualValues(t, 100, volumes["USD"]["input"]) {
		return
	}
	if !assert.EqualValues(t, 0, volumes["USD"]["output"]) {
		return
	}
}

func testFindAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	if !assert.NoError(t, err) {
		return
	}

	accounts, err := store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.EqualValues(t, 2, accounts.Total) {
		return
	}
	if !assert.True(t, accounts.HasMore) {
		return
	}
	if !assert.Equal(t, 1, accounts.PageSize) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
		After: accounts.Data.([]core.Account)[0].Address,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.EqualValues(t, 2, accounts.Total) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Equal(t, 1, accounts.PageSize) {
		return
	}
}

func testCountTransactions(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	countTransactions, err := store.CountTransactions(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 1, countTransactions)
}

func testFindTransactions(t *testing.T, store *sqlstorage.Store) {
	tx1 := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	tx2 := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	_, err := store.AppendLog(context.Background(), log1, log2)
	if !assert.NoError(t, err) {
		return
	}

	cursor, err := store.FindTransactions(context.Background(), query.Query{
		Limit: 1,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 1, cursor.PageSize) {
		return
	}
	if !assert.True(t, cursor.HasMore) {
		return
	}
	if !assert.EqualValues(t, 2, cursor.Total) {
		return
	}

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		After: fmt.Sprint(cursor.Data.([]core.Transaction)[0].ID),
		Limit: 1,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 1, cursor.PageSize) {
		return
	}
	if !assert.False(t, cursor.HasMore) {
		return
	}

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		Params: map[string]interface{}{
			"account":   "world",
			"reference": "tx1",
		},
		Limit: 1,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 1, cursor.PageSize) {
		return
	}
	if !assert.False(t, cursor.HasMore) {
		return
	}

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
	if !assert.NoError(t, err) {
		return
	}

	mapping, err := store.LoadMapping(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, mapping.Contracts, 1) {
		return
	}
	if !assert.EqualValues(t, m.Contracts[0], mapping.Contracts[0]) {
		return
	}

	m2 := core.Mapping{
		Contracts: []core.Contract{},
	}
	err = store.SaveMapping(context.Background(), m2)
	if !assert.NoError(t, err) {
		return
	}

	mapping, err = store.LoadMapping(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Len(t, mapping.Contracts, 0) {
		return
	}
}

func testGetTransaction(t *testing.T, store *sqlstorage.Store) {
	tx1 := core.Transaction{
		ID: uuid.New(),
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
			Metadata:  map[string]json.RawMessage{},
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	tx2 := core.Transaction{
		ID: uuid.New(),
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
			Metadata:  map[string]json.RawMessage{},
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	_, err := store.AppendLog(context.Background(), log1, log2)
	assert.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, tx1, tx) {
		return
	}
}

func testTooManyClient(t *testing.T, store *sqlstorage.Store) {

	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" { // Use of external server, ignore this test
		return
	}
	if ledgertesting.StorageDriverName() != "postgres" {
		return
	}

	for i := 0; i < pgtesting.MaxConnections; i++ {
		tx, err := store.Schema().BeginTx(context.Background(), nil)
		assert.NoError(t, err)
		defer tx.Rollback()
	}
	_, err := store.CountTransactions(context.Background())
	if !assert.Error(t, err) {
		return
	}
	if !assert.IsType(t, new(storage.Error), err) {
		return
	}
	if !assert.Equal(t, storage.TooManyClient, err.(*storage.Error).Code) {
		return
	}
}

func TestInitializeStore(t *testing.T) {

	l := logrus.New()
	l.Level = logrus.DebugLevel
	sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))

	driver, stopFn, err := ledgertesting.Driver()
	if !assert.NoError(t, err) {
		return
	}
	defer stopFn()
	defer driver.Close(context.Background())

	err = driver.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	store, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	if !assert.NoError(t, err) {
		return
	}

	modified, err := store.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	if !assert.True(t, modified) {

	}

	modified, err = store.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, modified) {
		return
	}
}

func testLastLog(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	log := core.NewTransactionLog(nil, tx)
	_, err := store.AppendLog(context.Background(), log)
	if !assert.NoError(t, err) {
		return
	}

	lastLog, err := store.LastLog(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	if !assert.NotNil(t, lastLog) {
		return
	}
	if !assert.EqualValues(t, tx, lastLog.Data) {
		return
	}
}
