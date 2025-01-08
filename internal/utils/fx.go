package utils

import (
	"context"
	"go.uber.org/fx"
)

func StartRunner[TYPE interface {
	Run(ctx context.Context)
}]() func(lc fx.Lifecycle, runner TYPE) {
	return func(lc fx.Lifecycle, runner TYPE) {
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
	}
}
