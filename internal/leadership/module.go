package leadership

import (
	"context"
	"github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"go.uber.org/fx"
	"time"
)

type ModuleConfig struct {
	LeadershipRetryPeriod time.Duration
}

func NewFXModule(config ModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(locker Locker, logger logging.Logger) *Manager {
			return NewManager(locker, logger, WithRetryPeriod(config.LeadershipRetryPeriod))
		}),
		fx.Provide(NewDefaultLocker),
		fx.Invoke(func(lc fx.Lifecycle, manager *Manager) {
			var (
				stopped = make(chan struct{})
			)
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						defer close(stopped)
						manager.Run(context.WithoutCancel(ctx))
					}()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					select {
					case <-stopped:
						return nil
					default:
						return manager.Stop(ctx)
					}
				},
			})
		}),
		fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, manager *Manager, logger logging.Logger, serviceHandlers []ServiceHandler) {
			services := collectionutils.Map(serviceHandlers, func(handler ServiceHandler) *Service {
				return NewService(manager, logger, handler)
			})
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					for _, s := range services {
						go s.Run(context.WithoutCancel(ctx))
					}

					return nil
				},
				OnStop: func(ctx context.Context) error {
					for _, s := range services {
						go s.Stop(ctx)
					}

					return nil
				},
			})
		}, fx.ParamTags(``, ``, ``, `group:"serviceHandlers"`))),
	)
}
