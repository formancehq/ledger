package opentelemetrytraces

import (
	"context"
	"fmt"
	"time"

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

func (o *openTelemetryStorage) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) (err error) {
	handlingErr := o.handle(ctx, "UpdateTransactionMetadata", func(ctx context.Context) error {
		err = o.underlying.UpdateTransactionMetadata(ctx, id, metadata, at)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry UpdateTransactionMetadata: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) UpdateAccountMetadata(ctx context.Context, id string, metadata core.Metadata, at time.Time) (err error) {
	handlingErr := o.handle(ctx, "UpdateAccountMetadata", func(ctx context.Context) error {
		err = o.underlying.UpdateAccountMetadata(ctx, id, metadata, at)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry UpdateAccountMetadata: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) Commit(ctx context.Context, txs ...core.ExpandedTransaction) (err error) {
	handlingErr := o.handle(ctx, "Commit", func(ctx context.Context) error {
		err = o.underlying.Commit(ctx, txs...)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry Commit: %s", handlingErr)
	}
	return
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

func (o *openTelemetryStorage) GetLastTransaction(ctx context.Context) (ret *core.ExpandedTransaction, err error) {
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

func (o *openTelemetryStorage) LastLog(ctx context.Context) (l *core.Log, err error) {
	handlingErr := o.handle(ctx, "LastLog", func(ctx context.Context) error {
		l, err = o.underlying.LastLog(ctx)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry LastLog: %s", handlingErr)
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

func (o *openTelemetryStorage) GetTransactions(ctx context.Context, query storage.TransactionsQuery) (q sharedapi.Cursor[core.ExpandedTransaction], err error) {
	handlingErr := o.handle(ctx, "GetTransactions", func(ctx context.Context) error {
		q, err = o.underlying.GetTransactions(ctx, query)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetTransactions: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetTransaction(ctx context.Context, txid uint64) (tx *core.ExpandedTransaction, err error) {
	handlingErr := o.handle(ctx, "GetTransaction", func(ctx context.Context) error {
		tx, err = o.underlying.GetTransaction(ctx, txid)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetTransaction: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAccount(ctx context.Context, accountAddress string) (acc *core.Account, err error) {
	handlingErr := o.handle(ctx, "GetAccount", func(ctx context.Context) error {
		acc, err = o.underlying.GetAccount(ctx, accountAddress)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAccount: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetAssetsVolumes(ctx context.Context, accountAddress string) (av core.AssetsVolumes, err error) {
	handlingErr := o.handle(ctx, "GetAssetsVolumes", func(ctx context.Context) error {
		av, err = o.underlying.GetAssetsVolumes(ctx, accountAddress)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetAssetsVolumes: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetVolumes(ctx context.Context, accountAddress, asset string) (v core.Volumes, err error) {
	handlingErr := o.handle(ctx, "GetVolumes", func(ctx context.Context) error {
		v, err = o.underlying.GetVolumes(ctx, accountAddress, asset)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetVolumes: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetBalances(ctx context.Context, q storage.BalancesQuery) (balances sharedapi.Cursor[core.AccountsBalances], err error) {
	handlingErr := o.handle(ctx, "GetBalances", func(ctx context.Context) error {
		balances, err = o.underlying.GetBalances(ctx, q)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetBalances: %s", handlingErr)
	}
	return
}

func (o *openTelemetryStorage) GetBalancesAggregated(ctx context.Context, q storage.BalancesQuery) (balances core.AssetsBalances, err error) {
	handlingErr := o.handle(ctx, "GetBalancesAggregated", func(ctx context.Context) error {
		balances, err = o.underlying.GetBalancesAggregated(ctx, q)
		return err
	})
	if handlingErr != nil {
		sharedlogging.Errorf("opentelemetry GetBalancesAggregated: %s", handlingErr)
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
	handlingErr := o.handle(ctx, "LoadMapping", func(ctx context.Context) error {
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
