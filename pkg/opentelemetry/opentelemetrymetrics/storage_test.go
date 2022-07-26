package opentelemetrymetrics

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/global"
)

func TestWrapStorageFactory(t *testing.T) {
	f := WrapStorageDriver(storage.NoOpDriver(), global.GetMeterProvider())
	store, _, err := f.GetStore(context.Background(), "bar", true)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)
}

func TestNewStorageDecorator(t *testing.T) {
	m := global.Meter("foo")

	transactionsCounter, err := transactionsCounter(m)
	assert.NoError(t, err)

	store := NewStorageDecorator(storage.NoOpStore(), transactionsCounter)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)

	err = store.Commit(context.Background(), core.ExpandedTransaction{})
	assert.NoError(t, err)
}
