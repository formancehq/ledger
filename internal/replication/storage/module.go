package storage

import (
	"context"

	"github.com/formancehq/ledger/internal/replication/drivers"

	"github.com/formancehq/ledger/internal/replication/controller"
	"github.com/formancehq/ledger/internal/replication/runner"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func Module(debug bool, connectionOptions bunconnect.ConnectionOptions) fx.Option {
	return fx.Options(
		bunconnect.Module(connectionOptions, debug),
		fx.Provide(fx.Annotate(NewPostgresStore,
			fx.As(new(runner.Store)),
			fx.As(new(controller.Store)),
			fx.As(new(drivers.Store)),
		)),
		fx.Invoke(func(lc fx.Lifecycle, db *bun.DB) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return errors.Wrap(NewMigrator(db).Up(ctx), "migrating database")
				},
			})
		}),
	)
}
