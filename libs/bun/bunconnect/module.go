package bunconnect

import (
	"context"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func Module(connectionOptions ConnectionOptions) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger) (*bun.DB, error) {
			return OpenSQLDB(logging.ContextWithLogger(context.Background(), logger), connectionOptions)
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
