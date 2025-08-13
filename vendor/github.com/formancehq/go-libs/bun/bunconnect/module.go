package bunconnect

import (
	"context"

	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/formancehq/go-libs/logging"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func Module(connectionOptions ConnectionOptions, debug bool) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger) (*bun.DB, error) {
			hooks := make([]bun.QueryHook, 0)
			if debug {
				hooks = append(hooks, bundebug.NewQueryHook())
			}

			logger.
				WithFields(map[string]any{
					"max-idle-conns":         connectionOptions.MaxIdleConns,
					"max-open-conns":         connectionOptions.MaxOpenConns,
					"max-conn-max-idle-time": connectionOptions.ConnMaxIdleTime,
				}).
				Infof("opening database connection")

			return OpenSQLDB(logging.ContextWithLogger(context.Background(), logger), connectionOptions, hooks...)
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
