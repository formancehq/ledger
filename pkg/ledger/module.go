package ledger

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"go.uber.org/fx"
)

type NumscriptCacheConfiguration struct {
	MaxCount int
}

type Configuration struct {
	NumscriptCache NumscriptCacheConfiguration
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
			return NewResolver(storageDriver, options...)
		}),
		fx.Provide(fx.Annotate(query.NewNoOpMonitor, fx.As(new(query.Monitor)))),
		fx.Provide(fx.Annotate(metrics.NewNoOpRegistry, fx.As(new(metrics.GlobalRegistry)))),
	)
}
