package driver

import (
	"context"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/logging"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(func(tracerProvider trace.TracerProvider) bucket.Factory {
			return bucket.NewDefaultFactory(bucket.WithTracer(tracerProvider.Tracer("store")))
		})),
		fx.Invoke(func(db *bun.DB) {
			db.Dialect().Tables().Register(
				&ledger.Transaction{},
				&ledger.Log{},
				&ledger.Account{},
				&ledger.Move{},
				&ledger.Ledger{},
			)
		}),
		fx.Provide(func(params struct {
			fx.In

			DB             *bun.DB
			TracerProvider trace.TracerProvider `optional:"true"`
			MeterProvider  metric.MeterProvider `optional:"true"`
		}) ledgerstore.Factory {
			options := make([]ledgerstore.Option, 0)
			if params.TracerProvider != nil {
				options = append(options, ledgerstore.WithTracer(params.TracerProvider.Tracer("store")))
			}
			if params.MeterProvider != nil {
				options = append(options, ledgerstore.WithMeter(params.MeterProvider.Meter("store")))
			}
			return ledgerstore.NewFactory(params.DB, options...)
		}),
		fx.Provide(func(
			db *bun.DB,
			bucketFactory bucket.Factory,
			ledgerStoreFactory ledgerstore.Factory,
			tracerProvider trace.TracerProvider,
		) (*Driver, error) {
			return New(
				db,
				ledgerStoreFactory,
				bucketFactory,
				systemstore.NewStoreFactory(systemstore.WithTracer(
					tracerProvider.Tracer("SystemStore"),
				)),
				WithTracer(tracerProvider.Tracer("StorageDriver")),
			), nil
		}),
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
