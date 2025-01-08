package runner

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/leadership"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"go.uber.org/fx"
	"time"
)

type ModuleConfig struct {
	SyncPeriod time.Duration
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
		fx.Provide(fx.Annotate(func(runner *Runner) leadership.ServiceHandler {
			return runner
		},
			fx.ResultTags(`group:"serviceHandlers"`),
		)),
		fx.Provide(func(registry *drivers.Registry) drivers.Factory {
			return registry
		}),
		// decorate the original Factory (implemented by *Registry)
		// to abstract the fact we want to batch logs
		fx.Decorate(fx.Annotate(
			drivers.NewWithBatchingConnectorFactory,
			fx.As(new(drivers.Factory)),
		)),
	)
}
