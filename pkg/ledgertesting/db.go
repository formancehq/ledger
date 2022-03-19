package ledgertesting

import (
	"context"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"os"
)

func StorageDriverName() string {
	fromEnv := os.Getenv("NUMARY_STORAGE_DRIVER")
	if fromEnv != "" {
		return fromEnv
	}
	return "sqlite"
}

func Driver() (storage.Driver, func(), error) {
	switch StorageDriverName() {
	case "sqlite":
		id := uuid.New()
		return sqlstorage.NewOpenCloseDBDriver("sqlite", sqlstorage.SQLite, func() (sqlstorage.DB, error) {
			return sqlstorage.NewSQLiteDB(os.TempDir(), id), nil
		}), func() {}, nil
	case "postgres":
		pgServer, err := pgtesting.PostgresServer()
		if err != nil {
			return nil, nil, err
		}
		return sqlstorage.NewOpenCloseDBDriver(
				"postgres",
				sqlstorage.PostgreSQL,
				func() (sqlstorage.DB, error) {
					db, err := sqlstorage.OpenSQLDB(sqlstorage.PostgreSQL, pgServer.ConnString())
					if err != nil {
						return nil, err
					}
					return sqlstorage.NewPostgresDB(db), nil
				},
			), func() {
				_ = pgServer.Close()
			}, nil
	}
	return nil, nil, errors.New("not found driver")
}

func StorageModule() fx.Option {
	return fx.Options(
		fx.Provide(func(lifecycle fx.Lifecycle) (storage.Driver, error) {
			driver, stopFn, err := Driver()
			if err != nil {
				return nil, err
			}
			lifecycle.Append(fx.Hook{
				OnStart: driver.Initialize,
				OnStop: func(ctx context.Context) error {
					stopFn()
					return driver.Close(ctx)
				},
			})
			return driver, nil
		}),
	)
}
