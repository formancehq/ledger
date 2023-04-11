package query

import (
	"context"

	"go.uber.org/fx"
)

func InitModule() fx.Option {
	return fx.Options(
		fx.Provide(NewDefaultInitLedgerConfig),
		fx.Provide(NewInitLedgers),
		fx.Invoke(func(lc fx.Lifecycle, initQuery *InitLedger) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return initLedgers(
						ctx,
						initQuery.cfg,
						initQuery.driver,
						initQuery.monitor,
						initQuery.metricsRegistry,
					)
				},
			})
		}),
	)
}
