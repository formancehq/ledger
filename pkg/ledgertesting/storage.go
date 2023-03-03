package ledgertesting

import (
	"context"

	"github.com/formancehq/ledger/internal/pgtesting"
	"github.com/formancehq/ledger/pkg/api/idempotency"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"go.uber.org/fx"
)

func StorageDriver() (*sqlstorage.Driver, func(), error) {
	pgServer, err := pgtesting.PostgresServer()
	if err != nil {
		return nil, nil, err
	}
	db, err := sqlstorage.OpenSQLDB(pgServer.ConnString())
	if err != nil {
		return nil, nil, err
	}
	return sqlstorage.NewDriver(
			"postgres",
			sqlstorage.NewPostgresDB(db),
		), func() {
			_ = pgServer.Close()
		}, nil
}

func ProvideStorageDriver() fx.Option {
	return fx.Provide(func(lc fx.Lifecycle) (*sqlstorage.Driver, error) {
		driver, stopFn, err := StorageDriver()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop: func(ctx context.Context) error {
				stopFn()
				return driver.Close(ctx)
			},
		})
		return driver, nil
	})
}

func ProvideLedgerStorageDriver() fx.Option {
	return fx.Options(
		ProvideStorageDriver(),
		fx.Provide(
			fx.Annotate(sqlstorage.NewLedgerStorageDriverFromRawDriver,
				fx.As(new(storage.Driver[ledger.Store]))),
			fx.Annotate(sqlstorage.NewIdempotencyStorageDriverFromRawDriver,
				fx.As(new(storage.Driver[idempotency.Store]))),
		),
	)
}
