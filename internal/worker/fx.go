package worker

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type ModuleConfig struct {
	Schedule     string
	MaxBlockSize int
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			db *bun.DB,
			traceProvider trace.TracerProvider,
		) (*AsyncBlockRunner, error) {
			parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
			schedule, err := parser.Parse(cfg.Schedule)
			if err != nil {
				return nil, err
			}

			return NewAsyncBlockRunner(logger, db, AsyncBlockRunnerConfig{
				MaxBlockSize: cfg.MaxBlockSize,
				Schedule:     schedule,
			}, WithTracer(traceProvider.Tracer("AsyncBlockRunner"))), nil
		}),
		fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, asyncBlockRunner *AsyncBlockRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := asyncBlockRunner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()

					return nil
				},
				OnStop: asyncBlockRunner.Stop,
			})
		}, fx.ParamTags(``, ``, ``, `group:"workerModules"`))),
	)
}
