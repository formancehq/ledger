package driver

import (
	"context"
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
		fx.Provide(func(
			db *bun.DB,
			tracerProvider trace.TracerProvider,
			meterProvider metric.MeterProvider,
		) (*Driver, error) {
			return New(db,
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
