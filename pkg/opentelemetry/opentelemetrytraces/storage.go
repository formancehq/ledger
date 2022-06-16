package opentelemetrytraces

import (
	"context"
	"fmt"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
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

func (o *openTelemetryStorage) GetLastTransaction(ctx context.Context) (ret *core.Transaction, err error) {
	handlingErr := o.handle(ctx, "GetLastTransaction", func(ctx context.Context) error {
		ret, err = o.underlying.GetLastTransaction(ctx)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetLastTransaction: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) Logs(ctx context.Context) (ret []core.Log, err error) {
	handlingErr := o.handle(ctx, "Logs", func(ctx context.Context) error {
		ret, err = o.underlying.Logs(ctx)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry Logs: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) AppendLog(ctx context.Context, logs ...core.Log) (err error) {
	handlingErr := o.handle(ctx, "AppendLog", func(ctx context.Context) error {
		err = o.underlying.AppendLog(ctx, logs...)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry AppendLogs: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) LastLog(ctx context.Context) (l *core.Log, err error) {
	handlingErr := o.handle(ctx, "LastLog", func(ctx context.Context) error {
		l, err = o.underlying.LastLog(ctx)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry LastLogs: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) CountTransactions(ctx context.Context, q storage.TransactionsQuery) (count uint64, err error) {
	handlingErr := o.handle(ctx, "CountTransactions", func(ctx context.Context) error {
		count, err = o.underlying.CountTransactions(ctx, q)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry CountTransactions: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetTransactions(ctx context.Context, query storage.TransactionsQuery) (q sharedapi.Cursor[core.Transaction], err error) {
	handlingErr := o.handle(ctx, "GetTransactions", func(ctx context.Context) error {
		q, err = o.underlying.GetTransactions(ctx, query)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetTransactions: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetTransaction(ctx context.Context, s uint64) (tx core.Transaction, err error) {
	handlingErr := o.handle(ctx, "GetTransaction", func(ctx context.Context) error {
		tx, err = o.underlying.GetTransaction(ctx, s)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetTransaction: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAccount(ctx context.Context, s string) (tx core.Account, err error) {
	handlingErr := o.handle(ctx, "GetAccount", func(ctx context.Context) error {
		tx, err = o.underlying.GetAccount(ctx, s)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAccount: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAccountVolumes(ctx context.Context, s string) (volumes core.Volumes, err error) {
	handlingErr := o.handle(ctx, "GetAccountVolumes", func(ctx context.Context) error {
		volumes, err = o.underlying.GetAccountVolumes(ctx, s)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAccountVolumes: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAccountVolume(ctx context.Context, account, asset string) (volume core.Volume, err error) {
	handlingErr := o.handle(ctx, "GetAccountVolume", func(ctx context.Context) error {
		volume, err = o.underlying.GetAccountVolume(ctx, account, asset)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAccountVolumes: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) CountAccounts(ctx context.Context, q storage.AccountsQuery) (count uint64, err error) {
	handlingErr := o.handle(ctx, "CountAccounts", func(ctx context.Context) error {
		count, err = o.underlying.CountAccounts(ctx, q)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry CountAccounts: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAccounts(ctx context.Context, query storage.AccountsQuery) (c sharedapi.Cursor[core.Account], err error) {
	handlingErr := o.handle(ctx, "GetAccounts", func(ctx context.Context) error {
		c, err = o.underlying.GetAccounts(ctx, query)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAccounts: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) LoadMapping(ctx context.Context) (m *core.Mapping, err error) {
	handlingErr := o.handle(ctx, "FindContracts", func(ctx context.Context) error {
		m, err = o.underlying.LoadMapping(ctx)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry LoadMapping: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return o.handle(ctx, "SaveMapping", func(ctx context.Context) error {
		return o.underlying.SaveMapping(ctx, mapping)
	})
}

func (o *openTelemetryStorage) Initialize(ctx context.Context) (ret bool, err error) {
	handlingErr := o.handle(ctx, "Initialize", func(ctx context.Context) error {
		ret, err = o.underlying.Initialize(ctx)
		return nil
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry Initialize: %s", handlingErr)
	}
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

type openTelemetryStorageDriver struct {
	storage.Driver
}

func (o openTelemetryStorageDriver) GetStore(ctx context.Context, name string, create bool) (storage.Store, bool, error) {
	store, created, err := o.Driver.GetStore(ctx, name, create)
	if err != nil {
		return nil, false, err
	}
	return NewStorageDecorator(store), created, nil
}

func WrapStorageDriver(underlying storage.Driver) *openTelemetryStorageDriver {
	return &openTelemetryStorageDriver{
		Driver: underlying,
	}
}
