package opentelemetrymetrics

import (
	"context"
	"errors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel/metric"
	"sync"
)

func transactionsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.NewInt64Counter("store/transactions")
}

func revertsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.NewInt64Counter("store/reverts")
}

type storageDecorator struct {
	storage.Store
	transactionsCounter metric.Int64Counter
	revertsCounter      metric.Int64Counter
}

func (o *storageDecorator) SaveTransactions(ctx context.Context, transactions []core.Transaction) error {
	err := o.Store.SaveTransactions(ctx, transactions)
	if err != nil {
		return err
	}
	add := 0
	reverts := 0
	for _, transaction := range transactions {
		if transaction.Metadata == nil {
			add++
			continue
		}
		if transaction.Metadata.IsReverted() {
			reverts++
			continue
		}
		add++
	}
	o.transactionsCounter.Add(context.Background(), int64(add))
	o.revertsCounter.Add(context.Background(), int64(reverts))
	return nil
}

var _ storage.Store = &storageDecorator{}

func NewStorageDecorator(underlying storage.Store, counter metric.Int64Counter, revertsCounter metric.Int64Counter) *storageDecorator {
	return &storageDecorator{
		Store:               underlying,
		transactionsCounter: counter,
		revertsCounter:      revertsCounter,
	}
}

type openTelemetryStorageFactory struct {
	underlying          storage.Factory
	meter               metric.Meter
	transactionsCounter metric.Int64Counter
	once                sync.Once
	revertsCounter      metric.Int64Counter
}

func (o *openTelemetryStorageFactory) GetStore(name string) (storage.Store, error) {
	var err error
	o.once.Do(func() {
		o.transactionsCounter, err = transactionsCounter(o.meter)
		if err != nil {
			return
		}
		o.revertsCounter, err = revertsCounter(o.meter)
	})
	if err != nil {
		return nil, errors.New("error creating meters")
	}
	store, err := o.underlying.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewStorageDecorator(store, o.transactionsCounter, o.revertsCounter), nil
}

func (o *openTelemetryStorageFactory) Close(ctx context.Context) error {
	return o.underlying.Close(ctx)
}

var _ storage.Factory = &openTelemetryStorageFactory{}

func WrapStorageFactory(underlying storage.Factory, mp metric.MeterProvider) *openTelemetryStorageFactory {
	return &openTelemetryStorageFactory{
		underlying: underlying,
		meter:      mp.Meter(opentelemetry.StoreInstrumentationName),
	}
}
