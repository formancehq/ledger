package ledger

import (
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage"
	"go.uber.org/fx"
)

func Module(allowPastTimestamp bool) fx.Option {
	return fx.Options(
		fx.Provide(func(
			storageDriver storage.Driver,
			monitor monitor.Monitor,
			metricsRegistry metrics.GlobalMetricsRegistry,
		) *Resolver {
			return NewResolver(storageDriver, monitor, allowPastTimestamp, metricsRegistry)
		}),
		fx.Provide(fx.Annotate(monitor.NewNoOpMonitor, fx.As(new(monitor.Monitor)))),
		fx.Provide(fx.Annotate(metrics.NewNoOpMetricsRegistry, fx.As(new(metrics.GlobalMetricsRegistry)))),
		query.QueryInitModule(),
	)
}
