package opentelemetrymetrics

import (
	"context"
	"errors"
	"sync"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
)

func transactionsCounter(m metric.Meter) (syncint64.Counter, error) {
	return m.SyncInt64().Counter(opentelemetry.StoreInstrumentationName + ".transactions")
}

type storageDecorator struct {
	ledger.Store
	transactionsCounter syncint64.Counter
}

func (o *storageDecorator) Commit(ctx context.Context, txs ...core.ExpandedTransaction) error {
	err := o.Store.Commit(ctx, txs...)
	if err != nil {
		return err
	}
	o.transactionsCounter.Add(context.Background(), int64(len(txs)))
	return nil
}

var _ ledger.Store = &storageDecorator{}

func NewStorageDecorator(underlying ledger.Store, counter syncint64.Counter) *storageDecorator {
	return &storageDecorator{
		Store:               underlying,
		transactionsCounter: counter,
	}
}

type openTelemetryStorageDriver struct {
	storage.Driver[ledger.Store]
	meter               metric.Meter
	transactionsCounter syncint64.Counter
	once                sync.Once
}

func (o *openTelemetryStorageDriver) GetLedgerStore(ctx context.Context, name string, create bool) (ledger.Store, bool, error) {
	var err error
	o.once.Do(func() {
		o.transactionsCounter, err = transactionsCounter(o.meter)
		if err != nil {
			return
		}
	})
	if err != nil {
		return nil, false, errors.New("error creating meters")
	}
	store, created, err := o.Driver.GetLedgerStore(ctx, name, create)
	if err != nil {
		return nil, false, err
	}
	return NewStorageDecorator(store, o.transactionsCounter), created, nil
}

func (o *openTelemetryStorageDriver) Close(ctx context.Context) error {
	return o.Driver.Close(ctx)
}

func WrapStorageDriver(underlying storage.Driver[ledger.Store], mp metric.MeterProvider) *openTelemetryStorageDriver {
	return &openTelemetryStorageDriver{
		Driver: underlying,
		meter:  mp.Meter(opentelemetry.StoreInstrumentationName),
	}
}
