package opentelemetrytraces

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
			name: "SaveTransactions",
			fn:   testSaveTransaction,
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

func testSaveTransaction(t *testing.T, store storage.Store) {
	txs := make([]core.Transaction, 0)
	_, err := store.SaveTransactions(context.Background(), txs)
	assert.NoError(t, err)
}

func testSaveMeta(t *testing.T, store storage.Store) {
	err := store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
		"transaction", "1", "firstname", "\"YYY\"")
	assert.NoError(t, err)
}

func testGetMeta(t *testing.T, store storage.Store) {
	_, err := store.GetMeta(context.TODO(), "transaction", "1")
	assert.NoError(t, err)
}

func testLastTransaction(t *testing.T, store storage.Store) {
	_, err := store.LastTransaction(context.Background())
	assert.NoError(t, err)

}

func testLastMetaID(t *testing.T, store storage.Store) {
	_, err := store.LastMetaID(context.Background())
	assert.NoError(t, err)
}

func testCountAccounts(t *testing.T, store storage.Store) {
	_, err := store.CountAccounts(context.Background())
	assert.NoError(t, err)

}

func testAggregateBalances(t *testing.T, store storage.Store) {
	_, err := store.AggregateBalances(context.Background(), "central_bank")
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

func testCountMeta(t *testing.T, store storage.Store) {
	_, err := store.CountMeta(context.Background())
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
	_, err := store.GetTransaction(context.Background(), "1")
	assert.NoError(t, err)
}
