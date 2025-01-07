package runner

import (
	"context"

	"github.com/formancehq/ledger/internal/replication/drivers"
	"go.uber.org/fx"
)

// NewModule create a new fx module
func NewModule() fx.Option {
	return fx.Options(
		fx.Provide(drivers.NewRegistry),
		fx.Provide(func(registry *drivers.Registry) drivers.Factory {
			return registry
		}),
		// decorate the original Factory (implemented by *Registry)
		// to abstract the fact we want to batch logs
		fx.Decorate(fx.Annotate(
			drivers.NewWithBatchingConnectorFactory,
			fx.As(new(drivers.Factory)),
		)),
		fx.Provide(NewRunner),
		fx.Invoke(func(lc fx.Lifecycle, runner *Runner) {
			lc.Append(fx.Hook{
				OnStart: runner.StartAsync,
				OnStop:  runner.Stop,
			})
		}),
		fx.Invoke(func(lc fx.Lifecycle, systemStore SystemStore, runner *Runner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return RestorePipelines(ctx, systemStore, runner)
				},
			})
		}),
	)
}

func As[T any](v T) T {
	return v
}
