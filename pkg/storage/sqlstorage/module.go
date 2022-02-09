package sqlstorage

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"path"
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

func DriverModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func() Flavor {
		return FlavorFromString(cfg.StorageDriver)
	}))
	options = append(options, fx.Provide(func(flavor Flavor) (storage.Driver, error) {
		var (
			cached             bool
			connStringResolver ConnStringResolver
			connString         string
		)
		switch flavor {
		case PostgreSQL:
			cached = true
			connString = cfg.PostgresConfig.ConnString
		case SQLite:
			connStringResolver = func(name string) string {
				return SQLiteFileConnString(path.Join(
					cfg.SQLiteConfig.Dir,
					fmt.Sprintf("%s_%s.db", cfg.SQLiteConfig.DBName, name),
				))
			}
		default:
			return nil, fmt.Errorf("nknown storage driver: %s", cfg.StorageDriver)
		}

		var driver storage.Driver
		if cached {
			driver = NewCachedDBDriver(logging.DefaultLogger(), flavor.String(), flavor, connString)
		} else {
			driver = NewOpenCloseDBDriver(logging.DefaultLogger(), flavor.String(), flavor, connStringResolver)
		}

		return driver, nil
	}))
	options = append(options, fx.Invoke(func(driver storage.Driver, lifecycle fx.Lifecycle) error {
		err := driver.Initialize(context.Background())
		if err != nil {
			return errors.Wrap(err, "initializing driver")
		}
		lifecycle.Append(fx.Hook{
			OnStop: driver.Close,
		})
		return nil
	}))
	return fx.Options(options...)
}
