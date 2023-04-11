package ledger

import (
	"time"

	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

type CacheConfiguration struct {
	EvictionRetainDelay time.Duration
	EvictionPeriod      time.Duration
}

type QueryConfiguration struct {
	LimitReadLogs int
}

type Configuration struct {
	AllowPastTimestamp bool
	Cache              CacheConfiguration
	Query              QueryConfiguration
}

func Module(configuration Configuration) fx.Option {
	return fx.Options(
		fx.Provide(func(
			storageDriver storage.Driver,
			monitor monitor.Monitor,
			metricsRegistry metrics.GlobalMetricsRegistry,
		) *Resolver {
			options := []option{
				WithMonitor(monitor),
				WithMetricsRegistry(metricsRegistry),
				WithCacheEvictionPeriod(configuration.Cache.EvictionPeriod),
				WithCacheEvictionRetainDelay(configuration.Cache.EvictionRetainDelay),
			}
			if configuration.AllowPastTimestamp {
				options = append(options, WithAllowPastTimestamps())
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
