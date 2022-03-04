package ledgertesting

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"os"
	"path"
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
		return sqlstorage.NewOpenCloseDBDriver("sqlite", sqlstorage.SQLite, func(name string) string {
			return sqlstorage.SQLiteFileConnString(path.Join(
				os.TempDir(),
				fmt.Sprintf("%s_%s.db", id, name),
			))
		}), func() {}, nil
	case "postgres":
		pgServer, err := PostgresServer()
		if err != nil {
			return nil, nil, err
		}
		return sqlstorage.NewOpenCloseDBDriver(
				"postgres",
				sqlstorage.PostgreSQL,
				func(name string) string {
					return pgServer.ConnString()
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
