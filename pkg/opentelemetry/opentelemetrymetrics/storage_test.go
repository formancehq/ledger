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
	f := WrapStorageFactory(storage.NoOpFactory(), global.GetMeterProvider())
	store, err := f.GetStore("bar")
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

	_, err = store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{}))
	assert.NoError(t, err)
}
