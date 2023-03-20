package query

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Supply(workerConfig{
			// TODO(gfyrag): Probably need to be configurable
			Interval: time.Second,
		}),
		fx.Provide(NewWorker),
		fx.Provide(fx.Annotate(NewNoOpMonitor, fx.As(new(Monitor)))),
		fx.Invoke(func(worker *Worker, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := worker.Run(logging.ContextWithLogger(
							context.Background(),
							logging.FromContext(ctx),
						)); err != nil {
							panic(err)
						}
					}()
					return nil
				},
				OnStop: worker.Stop,
			})
		}),
	)
}
