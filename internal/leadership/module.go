package leadership

import (
	"context"
	"go.uber.org/fx"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(NewManager),
		fx.Provide(NewDefaultLocker),
		fx.Invoke(func(lc fx.Lifecycle, runner *Manager) {
			var (
				stopped = make(chan struct{})
			)
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						defer close(stopped)
						runner.Run(context.WithoutCancel(ctx))
					}()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					select {
					case <-stopped:
						return nil
					default:
						return runner.Stop(ctx)
					}
				},
			})
		}),
	)
}
