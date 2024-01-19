package bunconnect

import (
	"context"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func Module(connectionOptions ConnectionOptions) fx.Option {
	return fx.Options(
		fx.Provide(func() (*bun.DB, error) {
			return OpenSQLDB(connectionOptions)
		}),
		fx.Invoke(func(lc fx.Lifecycle, db *bun.DB) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return db.Close()
				},
			})
		}),
	)
}
