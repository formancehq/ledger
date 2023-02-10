package sqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/numary/ledger/pkg/storage"
	"go.uber.org/fx"
)

type SQLiteConfig struct {
	Dir    string
	DBName string
}

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	StorageDriver  string
	SQLiteConfig   *SQLiteConfig
	PostgresConfig *PostgresConfig
}

func OpenSQLDB(flavor Flavor, dataSourceName string) (*sql.DB, error) {
	c, ok := sqlDrivers[flavor]
	if !ok {
		panic(fmt.Sprintf("Driver '%s' not found", flavor))
	}
	return sql.Open(c.driverName, dataSourceName)
}

func DriverModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func() Flavor {
		return FlavorFromString(cfg.StorageDriver)
	}))

	switch FlavorFromString(cfg.StorageDriver) {
	case PostgreSQL:
		options = append(options, fx.Provide(func() (*sql.DB, error) {
			return OpenSQLDB(PostgreSQL, cfg.PostgresConfig.ConnString)
		}))
		options = append(options, fx.Provide(func(db *sql.DB) DB {
			return NewPostgresDB(db)
		}))
		options = append(options, fx.Provide(func(db DB) (*Driver, error) {
			return NewDriver(PostgreSQL.String(), db), nil
		}))
		options = append(options, health.ProvideHealthCheck(func(db *sql.DB) health.NamedCheck {
			return health.NewNamedCheck(PostgreSQL.String(), health.CheckFn(db.PingContext))
		}))
	case SQLite:
		options = append(options, fx.Provide(func() DB {
			return NewSQLiteDB(cfg.SQLiteConfig.Dir, cfg.SQLiteConfig.DBName)
		}))
		options = append(options, fx.Provide(func(db DB) (*Driver, error) {
			return NewDriver(SQLite.String(), db), nil
		}))
		options = append(options, health.ProvideHealthCheck(func() health.NamedCheck {
			return health.NewNamedCheck(SQLite.String(), health.CheckFn(func(ctx context.Context) error {
				_, err := os.Open(cfg.SQLiteConfig.Dir)
				return err
			}))
		}))
	default:
		panic("Unsupported driver: " + cfg.StorageDriver)
	}
	options = append(options, fx.Provide(func(driver *Driver) storage.Driver[*Store] {
		return driver
	}))
	options = append(options, fx.Invoke(func(driver storage.Driver[*Store], lifecycle fx.Lifecycle) error {
		lifecycle.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop:  driver.Close,
		})
		return nil
	}))
	options = append(options, fx.Provide(
		NewLedgerStorageDriverFromRawDriver,
		NewDefaultStorageDriverFromRawDriver,
		NewIdempotencyStorageDriverFromRawDriver,
	))
	return fx.Options(options...)
}
