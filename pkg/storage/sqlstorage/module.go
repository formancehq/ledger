package sqlstorage

import (
	"context"
	"database/sql"
	"os"

	sharedhealth "github.com/numary/go-libs/sharedhealth/pkg"
	"github.com/numary/ledger/pkg/ledger"
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
		panic("PostgresSQL driver not found")
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
		options = append(options, sharedhealth.ProvideHealthCheck(func(db *sql.DB) sharedhealth.NamedCheck {
			return sharedhealth.NewNamedCheck(PostgreSQL.String(), sharedhealth.CheckFn(db.PingContext))
		}))
	case SQLite:
		options = append(options, fx.Provide(func() DB {
			return NewSQLiteDB(cfg.SQLiteConfig.Dir, cfg.SQLiteConfig.DBName)
		}))
		options = append(options, fx.Provide(func(db DB) (*Driver, error) {
			return NewDriver(SQLite.String(), db), nil
		}))
		options = append(options, sharedhealth.ProvideHealthCheck(func() sharedhealth.NamedCheck {
			return sharedhealth.NewNamedCheck(SQLite.String(), sharedhealth.CheckFn(func(ctx context.Context) error {
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
	options = append(options, fx.Provide(fx.Annotate(
		NewLedgerStorageDriverFromRawDriver,
		fx.As(new(storage.Driver[ledger.Store])),
	)))
	return fx.Options(options...)
}
