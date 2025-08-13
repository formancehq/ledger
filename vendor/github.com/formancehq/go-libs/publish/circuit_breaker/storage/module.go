package storage

import (
	"context"

	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/bun/bundebug"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func Module(schema string, storageLimit int, debug bool) fx.Option {
	options := make([]fx.Option, 0)

	options = append(options,
		fx.Provide(func(connectionOptions *bunconnect.ConnectionOptions, lc fx.Lifecycle) (Store, error) {

			hooks := make([]bun.QueryHook, 0)
			if debug {
				hooks = append(hooks, bundebug.NewQueryHook())
			}

			db, err := bunconnect.OpenDBWithSchema(context.Background(), *connectionOptions, schema, hooks...)
			if err != nil {
				return nil, err
			}

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return Migrate(ctx, schema, db)
				},
			})

			return New(schema, db, storageLimit), nil
		}),
	)

	return fx.Options(options...)
}
