package runner

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"go.uber.org/fx"
	"time"
)

type ModuleConfig struct {
	SyncPeriod      time.Duration
	PushRetryPeriod time.Duration
	PullInterval    time.Duration
}

// NewFXModule create a new fx module
func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewStorageAdapter, fx.As(new(Storage)))),
		fx.Provide(func(
			storageDriver Storage,
			connectorFactory drivers.Factory,
			logger logging.Logger,
		) *Runner {
			options := make([]Option, 0)
			if cfg.SyncPeriod > 0 {
				options = append(options, WithSyncPeriod(cfg.SyncPeriod))
			}
			if cfg.PushRetryPeriod > 0 {
				options = append(options, WithPipelineOptions(
					WithPushRetryPeriod(cfg.PushRetryPeriod),
				))
			}
			if cfg.PullInterval > 0 {
				options = append(options, WithPipelineOptions(
					WithPullPeriod(cfg.PullInterval),
				))
			}
			return NewRunner(
				storageDriver,
				connectorFactory,
				logger,
				options...,
			)
		}),
		fx.Provide(func(registry *drivers.Registry) drivers.Factory {
			return registry
		}),
		// decorate the original Factory (implemented by *Registry)
		// to abstract the fact we want to batch logs
		fx.Decorate(fx.Annotate(
			drivers.NewWithBatchingConnectorFactory,
			fx.As(new(drivers.Factory)),
		)),
		fx.Invoke(func(lc fx.Lifecycle, runner *Runner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go runner.Run(context.WithoutCancel(ctx))
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return runner.Stop(ctx)
				},
			})
		}),
	)
}
