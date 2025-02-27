package driver

import (
	"context"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	systemcontroller "github.com/formancehq/ledger/internal/controller/system"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v2/logging"
	"go.uber.org/fx"
)

type PostgresConfig struct {
	ConnString string
}

type ModuleConfiguration struct {
}

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(func(tracerProvider trace.TracerProvider) bucket.Factory {
			return bucket.NewDefaultFactory(bucket.WithTracer(tracerProvider.Tracer("store")))
		})),
		fx.Provide(func(db *bun.DB) systemstore.Store {
			return systemstore.New(db)
		}),
		fx.Provide(func(
			db *bun.DB,
			tracerProvider trace.TracerProvider,
			meterProvider metric.MeterProvider,
		) ledgerstore.Factory {
			return ledgerstore.NewFactory(db,
				ledgerstore.WithMeter(meterProvider.Meter("store")),
				ledgerstore.WithTracer(tracerProvider.Tracer("store")),
			)
		}),
		fx.Provide(func(
			db *bun.DB,
			bucketFactory bucket.Factory,
			ledgerStoreFactory ledgerstore.Factory,
			tracerProvider trace.TracerProvider,
			meterProvider metric.MeterProvider,
		) (*Driver, error) {
			return New(
				db,
				ledgerStoreFactory,
				bucketFactory,
				WithMeter(meterProvider.Meter("store")),
				WithTracer(tracerProvider.Tracer("store")),
			), nil
		}),
		fx.Provide(fx.Annotate(NewControllerStorageDriverAdapter, fx.As(new(systemcontroller.Store)))),
		fx.Invoke(func(driver *Driver, lifecycle fx.Lifecycle, logger logging.Logger) error {
			lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logger.Infof("Initializing database...")
					return driver.Initialize(ctx)
				},
			})
			return nil
		}),
	)
}
