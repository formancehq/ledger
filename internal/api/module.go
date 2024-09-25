package api

import (
	_ "embed"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/health"
	"github.com/formancehq/ledger/v2/internal/api/backend"
	"github.com/formancehq/ledger/v2/internal/engine"
	"github.com/formancehq/ledger/v2/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/v2/internal/storage/driver"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/fx"
)

type Config struct {
	Version  string
	ReadOnly bool
	Debug    bool
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			backend backend.Backend,
			healthController *health.HealthController,
			globalMetricsRegistry metrics.GlobalRegistry,
			a auth.Authenticator,
		) chi.Router {
			return NewRouter(backend, healthController, globalMetricsRegistry, a, cfg.ReadOnly, cfg.Debug)
		}),
		fx.Provide(func(storageDriver *driver.Driver, resolver *engine.Resolver) backend.Backend {
			return backend.NewDefaultBackend(storageDriver, cfg.Version, resolver)
		}),
		fx.Provide(fx.Annotate(noop.NewMeterProvider, fx.As(new(metric.MeterProvider)))),
		fx.Decorate(fx.Annotate(func(meterProvider metric.MeterProvider) (metrics.GlobalRegistry, error) {
			return metrics.RegisterGlobalRegistry(meterProvider)
		}, fx.As(new(metrics.GlobalRegistry)))),
		health.Module(),
	)
}
