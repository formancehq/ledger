package leadership

import (
	"context"
	"go.uber.org/fx"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(NewLeadership),
		fx.Provide(NewDefaultLocker),
		fx.Invoke(func(lc fx.Lifecycle, runner *Leadership) {
			var (
				cancel  context.CancelFunc
				stopped = make(chan struct{})
			)
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					ctx, cancel = context.WithCancel(context.WithoutCancel(ctx))
					go func() {
						defer close(stopped)
						runner.Run(ctx)
					}()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					cancel()
					select {
					case <-stopped:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			})
		}),
	)
}
