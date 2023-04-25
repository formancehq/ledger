package api

import (
	"context"
	_ "embed"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
)

type Config struct {
	Version string
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(routes.NewRouter),
		fx.Provide(func(storageDriver *storage.Driver, resolver *ledger.Resolver) controllers.Backend {
			return controllers.NewDefaultBackend(storageDriver, cfg.Version, resolver)
		}),
		//TODO(gfyrag): Move in pkg/ledger package
		fx.Invoke(func(lc fx.Lifecycle, backend controllers.Backend) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return backend.CloseLedgers(ctx)
				},
			})
		}),
		fx.Provide(fx.Annotate(metric.NewNoopMeterProvider, fx.As(new(metric.MeterProvider)))),
		fx.Decorate(fx.Annotate(func(meterProvider metric.MeterProvider) (metrics.GlobalMetricsRegistry, error) {
			return metrics.RegisterGlobalMetricsRegistry(meterProvider)
		}, fx.As(new(metrics.GlobalMetricsRegistry)))),
		health.Module(),
	)
}
