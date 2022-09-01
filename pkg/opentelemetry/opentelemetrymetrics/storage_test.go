package opentelemetrymetrics

import (
	"context"
	"testing"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/storage/noopstorage"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
)

func TestWrapStorageFactory(t *testing.T) {
	f := WrapStorageDriver(noopstorage.NoOpDriver(), metric.NewNoopMeterProvider())
	store, _, err := f.GetLedgerStore(context.Background(), "bar", true)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)
}

func TestNewStorageDecorator(t *testing.T) {
	m := global.Meter("foo")

	transactionsCounter, err := transactionsCounter(m)
	assert.NoError(t, err)

	store := NewStorageDecorator(noopstorage.NoOpStore(), transactionsCounter)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)

	err = store.Commit(context.Background(), core.ExpandedTransaction{})
	assert.NoError(t, err)
}
