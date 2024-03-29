package storage

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	"go.uber.org/fx"
)

func Module(schema string, storageLimit int) fx.Option {
	options := make([]fx.Option, 0)

	options = append(options,
		fx.Provide(func(connectionOptions *bunconnect.ConnectionOptions, lc fx.Lifecycle) (Store, error) {
			db, err := bunconnect.OpenDBWithSchema(context.Background(), *connectionOptions, schema)
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
