package opentelemetrymetrics

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/global"
	"testing"
)

func TestWrapStorageFactory(t *testing.T) {
	f := WrapStorageDriver(storage.NoOpDriver(), global.GetMeterProvider())
	store, _, err := f.GetStore(context.Background(), "bar")
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)
}

func TestNewStorageDecorator(t *testing.T) {
	m := global.Meter("foo")

	transactionsCounter, err := transactionsCounter(m)
	assert.NoError(t, err)
	revertsCounter, err := revertsCounter(m)
	assert.NoError(t, err)

	store := NewStorageDecorator(storage.NoOpStore(), transactionsCounter, revertsCounter)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)

	_, err = store.SaveTransactions(context.Background(), []core.Transaction{
		{}, {}, {},
	})
	assert.NoError(t, err)
}
