package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/uptrace/bun/extra/bunotel"

	"github.com/uptrace/bun/dialect/pgdialect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/logging"
	"go.uber.org/fx"
)

const dbName = "paymentsDB"

func Module(uri string) fx.Option {
	return fx.Options(
		fx.Provide(func() (*pgx.ConnConfig, error) {
			config, err := pgx.ParseConfig(uri)
			if err != nil {
				return nil, fmt.Errorf("parsing config: %w", err)
			}

			return config, nil
		}),

		fx.Provide(func(config *pgx.ConnConfig) *sql.DB {
			return stdlib.OpenDB(*config)
		}),

		fx.Provide(func(client *sql.DB) *Storage {
			db := bun.NewDB(client, pgdialect.New())

			db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithDBName(dbName)))

			return newStorage(db)
		}),

		fx.Invoke(func(lc fx.Lifecycle, repo *Storage) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.Debug("Ping database...")

					// TODO: Check migrations state and panic if migrations are not applied

					return nil
				},
			})
		}),
	)
}
