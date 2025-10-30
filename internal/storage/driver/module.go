package driver

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/logging"
	"go.uber.org/fx"
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
		// SystemStoreFactory is provided separately to be used both by the Driver
		// and by the ledger store factory for counting ledgers in buckets
		fx.Provide(func(tracerProvider trace.TracerProvider) systemstore.StoreFactory {
			return systemstore.NewStoreFactory(systemstore.WithTracer(
				tracerProvider.Tracer("SystemStore"),
			))
		}),
		fx.Provide(func(params struct {
			fx.In

			DB                 *bun.DB
			SystemStoreFactory systemstore.StoreFactory
			TracerProvider     trace.TracerProvider `optional:"true"`
			MeterProvider      metric.MeterProvider `optional:"true"`
		}) ledgerstore.Factory {
			options := make([]ledgerstore.Option, 0)
			if params.TracerProvider != nil {
				options = append(options, ledgerstore.WithTracer(params.TracerProvider.Tracer("store")))
			}
			if params.MeterProvider != nil {
				options = append(options, ledgerstore.WithMeter(params.MeterProvider.Meter("store")))
			}
			options = append(options, ledgerstore.WithCountLedgersInBucketFunc(
				func(ctx context.Context, bucketName string) (int, error) {
					return params.SystemStoreFactory.Create(params.DB).CountLedgersInBucket(ctx, bucketName)
				},
			))
			return ledgerstore.NewFactory(params.DB, options...)
		}),
		fx.Provide(func(
			db *bun.DB,
			bucketFactory bucket.Factory,
			ledgerStoreFactory ledgerstore.Factory,
			systemStoreFactory systemstore.StoreFactory,
			tracerProvider trace.TracerProvider,
		) (*Driver, error) {
			return New(
				db,
				ledgerStoreFactory,
				bucketFactory,
				systemStoreFactory,
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
