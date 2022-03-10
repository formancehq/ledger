package opentelemetrytraces

import (
	"context"
	"fmt"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type openTelemetryStorage struct {
	underlying storage.Store
}

func (o *openTelemetryStorage) handle(ctx context.Context, name string, fn func(ctx context.Context) error) error {
	ctx, span := otel.Tracer(opentelemetry.StoreInstrumentationName).Start(ctx, name)
	defer span.End(trace.WithStackTrace(true))
	defer func() {
		if e := recover(); e != nil {
			defer func() {
				panic(e)
			}()
			span.SetStatus(codes.Error, fmt.Sprintf("%s", e))
		}
	}()

	span.SetAttributes(attribute.String("ledger", o.Name()))
	err := fn(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (o *openTelemetryStorage) AppendLog(ctx context.Context, logs ...core.Log) (ret map[int]error, err error) {
	o.handle(ctx, "AppendLog", func(ctx context.Context) error {
		ret, err = o.underlying.AppendLog(ctx, logs...)
		return err
	})
	return
}

func (o *openTelemetryStorage) LastLog(ctx context.Context) (l *core.Log, err error) {
	o.handle(ctx, "LastLog", func(ctx context.Context) error {
		l, err = o.underlying.LastLog(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) CountTransactions(ctx context.Context) (count int64, err error) {
	o.handle(ctx, "CountTransactions", func(ctx context.Context) error {
		count, err = o.underlying.CountTransactions(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) FindTransactions(ctx context.Context, query query.Query) (q sharedapi.Cursor, err error) {
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

func (o *openTelemetryStorage) GetAccount(ctx context.Context, s string) (tx core.Account, err error) {
	o.handle(ctx, "GetAccount", func(ctx context.Context) error {
		tx, err = o.underlying.GetAccount(ctx, s)
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

func (o *openTelemetryStorage) FindAccounts(ctx context.Context, query query.Query) (q sharedapi.Cursor, err error) {
	o.handle(ctx, "FindAccounts", func(ctx context.Context) error {
		q, err = o.underlying.FindAccounts(ctx, query)
		return err
	})
	return
}

func (o *openTelemetryStorage) LoadMapping(ctx context.Context) (m *core.Mapping, err error) {
	o.handle(ctx, "FindContracts", func(ctx context.Context) error {
		m, err = o.underlying.LoadMapping(ctx)
		return err
	})
	return
}

func (o *openTelemetryStorage) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return o.handle(ctx, "SaveMapping", func(ctx context.Context) error {
		return o.underlying.SaveMapping(ctx, mapping)
	})
}

func (o *openTelemetryStorage) Initialize(ctx context.Context) (ret bool, err error) {
	o.handle(ctx, "Initialize", func(ctx context.Context) error {
		ret, err = o.underlying.Initialize(ctx)
		return nil
	})
	return
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

func NewStorageDecorator(underlying storage.Store) *openTelemetryStorage {
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
	return NewStorageDecorator(store), nil
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
