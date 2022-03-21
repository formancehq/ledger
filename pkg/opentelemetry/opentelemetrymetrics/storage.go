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
	return m.NewInt64Counter(opentelemetry.StoreInstrumentationName + ".transactions")
}

func revertsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.NewInt64Counter(opentelemetry.StoreInstrumentationName + ".reverts")
}

type storageDecorator struct {
	storage.Store
	transactionsCounter metric.Int64Counter
	revertsCounter      metric.Int64Counter
}

func (o *storageDecorator) SaveTransactions(ctx context.Context, transactions []core.Transaction) (map[int]error, error) {
	ret, err := o.Store.SaveTransactions(ctx, transactions)
	if err != nil {
		return ret, err
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
	return ret, nil
}

var _ storage.Store = &storageDecorator{}

func NewStorageDecorator(underlying storage.Store, counter metric.Int64Counter, revertsCounter metric.Int64Counter) *storageDecorator {
	return &storageDecorator{
		Store:               underlying,
		transactionsCounter: counter,
		revertsCounter:      revertsCounter,
	}
}

type openTelemetryStorageDriver struct {
	storage.Driver
	meter               metric.Meter
	transactionsCounter metric.Int64Counter
	once                sync.Once
	revertsCounter      metric.Int64Counter
}

func (o *openTelemetryStorageDriver) GetStore(ctx context.Context, name string, create bool) (storage.Store, bool, error) {
	var err error
	o.once.Do(func() {
		o.transactionsCounter, err = transactionsCounter(o.meter)
		if err != nil {
			return
		}
		o.revertsCounter, err = revertsCounter(o.meter)
	})
	if err != nil {
		return nil, false, errors.New("error creating meters")
	}
	store, created, err := o.Driver.GetStore(ctx, name, create)
	if err != nil {
		return nil, false, err
	}
	return NewStorageDecorator(store, o.transactionsCounter, o.revertsCounter), created, nil
}

func (o *openTelemetryStorageDriver) Close(ctx context.Context) error {
	return o.Driver.Close(ctx)
}

func WrapStorageDriver(underlying storage.Driver, mp metric.MeterProvider) *openTelemetryStorageDriver {
	return &openTelemetryStorageDriver{
		Driver: underlying,
		meter:  mp.Meter(opentelemetry.StoreInstrumentationName),
	}
}
