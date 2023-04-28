package ledger

import (
	"github.com/formancehq/ledger/pkg/ledger/command"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

type NumscriptCacheConfiguration struct {
	MaxCount int
}

type QueryConfiguration struct {
	LimitReadLogs int
}

type Configuration struct {
	NumscriptCache NumscriptCacheConfiguration
	Query          QueryConfiguration
}

func Module(configuration Configuration) fx.Option {
	return fx.Options(
		fx.Provide(func(
			storageDriver *storage.Driver,
			monitor monitor.Monitor,
			metricsRegistry metrics.GlobalMetricsRegistry,
		) *Resolver {
			options := []option{
				WithMonitor(monitor),
				WithMetricsRegistry(metricsRegistry),
			}
			if configuration.NumscriptCache.MaxCount != 0 {
				options = append(options, WithCompiler(command.NewCompiler(configuration.NumscriptCache.MaxCount)))
			}
			return NewResolver(storageDriver, options...)
		}),
		fx.Provide(fx.Annotate(monitor.NewNoOpMonitor, fx.As(new(monitor.Monitor)))),
		fx.Provide(fx.Annotate(metrics.NewNoOpMetricsRegistry, fx.As(new(metrics.GlobalMetricsRegistry)))),
		query.InitModule(),
		fx.Decorate(func() *query.InitLedgerConfig {
			return query.NewInitLedgerConfig(configuration.Query.LimitReadLogs)
		}),
	)
}
