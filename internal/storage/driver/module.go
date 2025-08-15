package driver

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
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
				&ledger.Ledger{},
			)
		}),
		ledgerstore.NewModule(),
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
