package opentelemetrytraces

import (
	"context"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type openTelemetryStorage struct {
	underlying storage.Store
}

func (o *openTelemetryStorage) handle(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	ctx, span := otel.Tracer("Store").Start(ctx, name)
	defer span.End()

	span.SetAttributes(attribute.String("ledger", o.Name()))
	err := fn(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (o *openTelemetryStorage) LastTransaction(ctx context.Context) (tx *core.Transaction, err error) {
	o.handle(ctx, "LastTransaction", func(ctx context.Context) error {
		tx, err = o.underlying.LastTransaction(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) LastMetaID(ctx context.Context) (count int64, err error) {
	o.handle(ctx, "LastMetaID", func(ctx context.Context) error {
		count, err = o.underlying.LastMetaID(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) SaveTransactions(ctx context.Context, transactions []core.Transaction) error {
	return o.handle(ctx, "SaveTransactions", func(ctx context.Context) error {
		return o.underlying.SaveTransactions(ctx, transactions)
	})
}

func (o *openTelemetryStorage) CountTransactions(ctx context.Context) (count int64, err error) {
	o.handle(ctx, "CountTransactions", func(ctx context.Context) error {
		count, err = o.underlying.CountTransactions(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) FindTransactions(ctx context.Context, query query.Query) (q query.Cursor, err error) {
	o.handle(ctx, "FindTransactions", func(ctx context.Context) error {
		q, err = o.underlying.FindTransactions(ctx, query)
		return err
	})
	return
}

func (o *openTelemetryStorage) GetTransaction(ctx context.Context, s string) (tx core.Transaction, err error) {
	o.handle(ctx, "GetTransaction", func(ctx context.Context) error {
		tx, err = o.underlying.GetTransaction(ctx, s)
		return err
	})
	return
}

func (o *openTelemetryStorage) AggregateBalances(ctx context.Context, s string) (balances map[string]int64, err error) {
	o.handle(ctx, "AggregateBalances", func(ctx context.Context) error {
		balances, err = o.underlying.AggregateBalances(ctx, s)
		return err
	})
	return
}

func (o *openTelemetryStorage) AggregateVolumes(ctx context.Context, s string) (balances map[string]map[string]int64, err error) {
	o.handle(ctx, "AggregateVolumes", func(ctx context.Context) error {
		balances, err = o.underlying.AggregateVolumes(ctx, s)
		return err
	})
	return
}

func (o *openTelemetryStorage) CountAccounts(ctx context.Context) (count int64, err error) {
	o.handle(ctx, "CountAccounts", func(ctx context.Context) error {
		count, err = o.underlying.CountAccounts(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) FindAccounts(ctx context.Context, query query.Query) (q query.Cursor, err error) {
	o.handle(ctx, "FindAccounts", func(ctx context.Context) error {
		q, err = o.underlying.FindAccounts(ctx, query)
		return err
	})
	return
}

func (o *openTelemetryStorage) SaveMeta(ctx context.Context, i int64, s string, s2 string, s3 string, s4 string, s5 string) error {
	return o.handle(ctx, "SaveMeta", func(ctx context.Context) error {
		return o.underlying.SaveMeta(ctx, i, s, s2, s3, s4, s5)
	})
}

func (o *openTelemetryStorage) GetMeta(ctx context.Context, s string, s2 string) (m core.Metadata, err error) {
	o.handle(ctx, "GetMeta", func(ctx context.Context) error {
		m, err = o.underlying.GetMeta(ctx, s, s2)
		return err
	})
	return
}

func (o *openTelemetryStorage) CountMeta(ctx context.Context) (count int64, err error) {
	o.handle(ctx, "CountMeta", func(ctx context.Context) error {
		count, err = o.underlying.CountMeta(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) Initialize(ctx context.Context) error {
	return o.handle(ctx, "Initialize", func(ctx context.Context) error {
		return o.underlying.Initialize(ctx)
	})
}

func (o *openTelemetryStorage) Name() string {
	return o.underlying.Name()
}

func (o *openTelemetryStorage) Close(ctx context.Context) error {
	return o.handle(ctx, "Close", func(ctx context.Context) error {
		return o.underlying.Close(ctx)
	})
}

var _ storage.Store = &openTelemetryStorage{}

func NewOpenTelemetryStorage(underlying storage.Store) *openTelemetryStorage {
	return &openTelemetryStorage{
		underlying: underlying,
	}
}

type openTelemetryStorageFactory struct {
	underlying storage.Factory
}

func (o openTelemetryStorageFactory) GetStore(name string) (storage.Store, error) {
	store, err := o.underlying.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewOpenTelemetryStorage(store), nil
}

func (o openTelemetryStorageFactory) Close(ctx context.Context) error {
	return o.underlying.Close(ctx)
}

var _ storage.Factory = &openTelemetryStorageFactory{}

func WrapStorageFactory(underlying storage.Factory) *openTelemetryStorageFactory {
	return &openTelemetryStorageFactory{
		underlying: underlying,
	}
}
