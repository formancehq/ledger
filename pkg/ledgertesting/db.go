package ledgertesting

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"os"
	"path"
)

func TestingModule() fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, lifecycle fx.Lifecycle) (storage.Driver, error) {
			var driver storage.Driver
			id := uuid.New()
			switch os.Getenv("NUMARY_STORAGE_DRIVER") {
			case "sqlite", "":
				driver = sqlstorage.NewOpenCloseDBDriver(logger, "sqlite", sqlstorage.SQLite, func(name string) string {
					return sqlstorage.SQLiteFileConnString(path.Join(
						os.TempDir(),
						fmt.Sprintf("%s_%s.db", id, name),
					))
				})
			case "postgres":
				pgServer, err := PostgresServer()
				if err != nil {
					return nil, err
				}
				lifecycle.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						return pgServer.Close()
					},
				})
				driver = sqlstorage.NewOpenCloseDBDriver(
					logger,
					"postgres",
					sqlstorage.PostgreSQL,
					func(name string) string {
						return pgServer.ConnString()
					},
				)
			}
			if driver == nil {
				return nil, errors.New("not found driver")
			}
			return driver, nil
		}),
		fx.Invoke(func(driver storage.Driver, lifecycle fx.Lifecycle) error {
			err := driver.Initialize(context.Background())
			if err != nil {
				return err
			}
			lifecycle.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return driver.Close(ctx)
				},
			})
			return nil
		}),
	)
}
