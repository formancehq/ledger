package opentelemetrytraces

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	type testingFunction struct {
		name string
		fn   func(t *testing.T, store storage.Store)
	}

	for _, tf := range []testingFunction{
		{
			name: "AppendLog",
			fn:   testAppendLog,
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
			name: "GetAccounts",
			fn:   testGetAccounts,
		},
		{
			name: "CountTransactions",
			fn:   testCountTransactions,
		},
		{
			name: "GetTransactions",
			fn:   testGetTransactions,
		},
		{
			name: "GetTransaction",
			fn:   testGetTransaction,
		},
	} {
		t.Run(tf.name, func(t *testing.T) {
			store := NewStorageDecorator(storage.NoOpStore())
			defer func(store *openTelemetryStorage, ctx context.Context) {
				if err := store.Close(ctx); err != nil {
					panic(err)
				}
			}(store, context.Background())

			_, err := store.Initialize(context.Background())
			assert.NoError(t, err)

			tf.fn(t, store)
		})
	}
}

func testAppendLog(t *testing.T, store storage.Store) {
	err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{}))
	assert.NoError(t, err)
}

func testLastLog(t *testing.T, store storage.Store) {
	_, err := store.LastLog(context.Background())
	assert.NoError(t, err)
}

func testCountAccounts(t *testing.T, store storage.Store) {
	_, err := store.CountAccounts(context.Background(), query.Accounts{})
	assert.NoError(t, err)

}

func testAggregateVolumes(t *testing.T, store storage.Store) {
	_, err := store.AggregateVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
}

func testGetAccounts(t *testing.T, store storage.Store) {
	_, err := store.GetAccounts(context.Background(), query.Accounts{
		Limit: 1,
	})
	assert.NoError(t, err)
}

func testCountTransactions(t *testing.T, store storage.Store) {
	_, err := store.CountTransactions(context.Background(), query.Transactions{})
	assert.NoError(t, err)
}

func testGetTransactions(t *testing.T, store storage.Store) {
	_, err := store.GetTransactions(context.Background(), query.Transactions{
		Limit: 1,
	})
	assert.NoError(t, err)
}

func testGetTransaction(t *testing.T, store storage.Store) {
	_, err := store.GetTransaction(context.Background(), 1)
	assert.NoError(t, err)
}
