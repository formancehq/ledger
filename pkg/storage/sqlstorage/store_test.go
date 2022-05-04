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
				t.Fatal("timeout")
			case <-done:
			}
		})
	}

}

func testSaveTransaction(t *testing.T, store *sqlstorage.Store) {
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}))
	assert.NoError(t, err)
}

func testDuplicatedTransaction(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: 0,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	}
	log1 := core.NewTransactionLog(nil, tx)
	err := store.AppendLog(context.Background(), log1)
	assert.NoError(t, err)

	log2 := core.NewTransactionLog(&log1, core.Transaction{
		ID: 1,
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	})
	err = store.AppendLog(context.Background(), log2)
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
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	if !assert.NoError(t, err) {
		return
	}

	countAccounts, err := store.CountAccounts(context.Background(), query.Query{})
	if !assert.EqualValues(t, 2, countAccounts) { // world + central_bank
		return
	}
}

func testAggregateVolumes(t *testing.T, store *sqlstorage.Store) {
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
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
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
	if !assert.NoError(t, err) {
		return
	}

	accounts, err := store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
	})
	if !assert.NoError(t, err) {
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
	if !assert.True(t, accounts.HasMore) {
		return
	}
	if !assert.Equal(t, 1, accounts.PageSize) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 10,
		Params: map[string]interface{}{
			"address": ".*der.*",
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Len(t, accounts.Data, 2) {
		return
	}
	if !assert.Equal(t, 10, accounts.PageSize) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 10,
		Params: map[string]interface{}{
			"metadata": map[string]string{
				"foo": "bar",
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Len(t, accounts.Data, 1) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 10,
		Params: map[string]interface{}{
			"metadata": map[string]string{
				"number": "3",
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Len(t, accounts.Data, 1) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 10,
		Params: map[string]interface{}{
			"metadata": map[string]string{
				"boolean": "true",
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Len(t, accounts.Data, 1) {
		return
	}

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 10,
		Params: map[string]interface{}{
			"metadata": map[string]string{
				"a.super.nested.key": "hello",
			},
		},
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.False(t, accounts.HasMore) {
		return
	}
	if !assert.Len(t, accounts.Data, 1) {
		return
	}
}

func testCountTransactions(t *testing.T, store *sqlstorage.Store) {
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
			Metadata: map[string]json.RawMessage{
				"lastname": json.RawMessage(`"XXX"`),
			},
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	if !assert.NoError(t, err) {
		return
	}

	countTransactions, err := store.CountTransactions(context.Background(), query.Query{})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.EqualValues(t, 1, countTransactions) {
		return
	}
}

func testFindTransactions(t *testing.T, store *sqlstorage.Store) {
	tx1 := core.Transaction{
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
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	tx3 := core.Transaction{
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
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	log3 := core.NewTransactionLog(&log2, tx3)
	err := store.AppendLog(context.Background(), log1, log2, log3)
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
	if !assert.True(t, cursor.HasMore) {
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
	if !assert.Len(t, cursor.Data, 1) {
		return
	}
	if !assert.False(t, cursor.HasMore) {
		return
	}

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		Params: map[string]interface{}{
			"source": "central_bank",
		},
		Limit: 10,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 10, cursor.PageSize) {
		return
	}
	if !assert.Len(t, cursor.Data, 1) {
		return
	}
	if !assert.False(t, cursor.HasMore) {
		return
	}

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		Params: map[string]interface{}{
			"destination": "users:1",
		},
		Limit: 10,
	})
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equal(t, 10, cursor.PageSize) {
		return
	}
	if !assert.Len(t, cursor.Data, 1) {
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
		Timestamp: time.Now().UTC().Round(time.Second).Format(time.RFC3339),
	}
	tx2 := core.Transaction{
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
			Metadata:  map[string]json.RawMessage{},
		},
		Timestamp: time.Now().UTC().Round(time.Second).Format(time.RFC3339),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	err := store.AppendLog(context.Background(), log1, log2)
	if !assert.NoError(t, err) {
		return
	}

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
	_, err := store.CountTransactions(context.Background(), query.Query{})
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
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Metadata: core.Metadata{},
		},
		Timestamp: time.Now().Round(time.Second).Format(time.RFC3339),
	}
	log := core.NewTransactionLog(nil, tx)
	err := store.AppendLog(context.Background(), log)
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
