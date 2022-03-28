package opentelemetrytraces

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
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
	} {
		t.Run(tf.name, func(t *testing.T) {
			store := NewStorageDecorator(storage.NoOpStore())
			defer store.Close(context.Background())

			_, err := store.Initialize(context.Background())
			assert.NoError(t, err)

			tf.fn(t, store)
		})
	}
}

func testAppendLog(t *testing.T, store storage.Store) {
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{}))
	assert.NoError(t, err)
}

func testLastLog(t *testing.T, store storage.Store) {
	_, err := store.LastLog(context.Background())
	assert.NoError(t, err)
}

func testCountAccounts(t *testing.T, store storage.Store) {
	_, err := store.CountAccounts(context.Background())
	assert.NoError(t, err)

}

func testAggregateVolumes(t *testing.T, store storage.Store) {
	_, err := store.AggregateVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
}

func testFindAccounts(t *testing.T, store storage.Store) {
	_, err := store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
}

func testCountTransactions(t *testing.T, store storage.Store) {
	_, err := store.CountTransactions(context.Background())
	assert.NoError(t, err)
}

func testFindTransactions(t *testing.T, store storage.Store) {
	_, err := store.FindTransactions(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
}

func testGetTransaction(t *testing.T, store storage.Store) {
	_, err := store.GetTransaction(context.Background(), 1)
	assert.NoError(t, err)
}
