package engine

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/ledger/internal/storage/driver"
	"go.uber.org/fx"
)

type NumscriptCacheConfiguration struct {
	MaxCount int
}

type Configuration struct {
	NumscriptCache  NumscriptCacheConfiguration
	LedgerBatchSize int
}

func Module(configuration Configuration) fx.Option {
	return fx.Options(
		fx.Provide(func(
			storageDriver *driver.Driver,
			publisher message.Publisher,
			metricsRegistry metrics.GlobalRegistry,
			logger logging.Logger,
		) *Resolver {
			options := []option{
				WithMessagePublisher(publisher),
				WithMetricsRegistry(metricsRegistry),
				WithLogger(logger),
			}
			if configuration.NumscriptCache.MaxCount != 0 {
				options = append(options, WithCompiler(command.NewCompiler(configuration.NumscriptCache.MaxCount)))
			}
			if configuration.LedgerBatchSize != 0 {
				options = append(options, WithLedgerConfig(GlobalLedgerConfig{
					batchSize: configuration.LedgerBatchSize,
				}))
			}
			return NewResolver(storageDriver, options...)
		}),
		fx.Provide(fx.Annotate(bus.NewNoOpMonitor, fx.As(new(bus.Monitor)))),
		fx.Provide(fx.Annotate(metrics.NewNoOpRegistry, fx.As(new(metrics.GlobalRegistry)))),
		//TODO(gfyrag): Move in pkg/ledger package
		fx.Invoke(func(lc fx.Lifecycle, resolver *Resolver) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return resolver.CloseLedgers(ctx)
				},
			})
		}),
	)
}
