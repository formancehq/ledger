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

func (o *storageDecorator) AppendLog(ctx context.Context, logs ...core.Log) (map[int]error, error) {
	ret, err := o.Store.AppendLog(ctx, logs...)
	if err != nil {
		return ret, err
	}
	add := 0
	reverts := 0
	for _, log := range logs {
		switch tx := log.Data.(type) {
		case core.Transaction:
			if tx.Metadata == nil {
				add++
				continue
			}
			if tx.Metadata.IsReverted() {
				reverts++
				continue
			}
			add++
		}

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
