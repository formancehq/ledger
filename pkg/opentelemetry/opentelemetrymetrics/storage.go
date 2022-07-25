package opentelemetrymetrics

import (
	"context"
	"errors"
	"sync"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel/metric"
)

func transactionsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.NewInt64Counter(opentelemetry.StoreInstrumentationName + ".transactions")
}

func revertsCounter(m metric.Meter) (metric.Int64Counter, error) {
	return m.NewInt64Counter(opentelemetry.StoreInstrumentationName + ".reverts")
}

type storageDecorator struct {
	storage.LedgerStore
	transactionsCounter metric.Int64Counter
	revertsCounter      metric.Int64Counter
}

func (o *storageDecorator) AppendLog(ctx context.Context, logs ...core.Log) error {
	err := o.LedgerStore.AppendLog(ctx, logs...)
	if err != nil {
		return err
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
	return nil
}

var _ storage.LedgerStore = &storageDecorator{}

func NewStorageDecorator(underlying storage.LedgerStore, counter metric.Int64Counter, revertsCounter metric.Int64Counter) *storageDecorator {
	return &storageDecorator{
		LedgerStore:         underlying,
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

func (o *openTelemetryStorageDriver) GetLedgerStore(ctx context.Context, name string, create bool) (storage.LedgerStore, bool, error) {
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
	store, created, err := o.Driver.GetLedgerStore(ctx, name, create)
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
