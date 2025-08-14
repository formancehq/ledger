package lockmonitor

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func NewModule(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(func(logger logging.Logger, db *bun.DB, options ...Option) *Worker {
			return NewWorker(logger, db, cfg, options...)
		}, fx.ParamTags(``, ``, `group:"lockmonitor.options"`))),
		fx.Invoke(func(lc fx.Lifecycle, worker *Worker) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := worker.Run(context.WithoutCancel(ctx)); err != nil {
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
