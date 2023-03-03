package sqlstorage

import (
	"database/sql"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"go.uber.org/fx"
)

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	PostgresConfig *PostgresConfig
}

func OpenSQLDB(dataSourceName string) (*sql.DB, error) {
	return sql.Open("pgx", dataSourceName)
}

func DriverModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)

	options = append(options, fx.Provide(func() (*sql.DB, error) {
		return OpenSQLDB(cfg.PostgresConfig.ConnString)
	}))
	options = append(options, fx.Provide(func(db *sql.DB) DB {
		return NewPostgresDB(db)
	}))
	options = append(options, fx.Provide(func(db DB) (*Driver, error) {
		return NewDriver("postgres", db), nil
	}))
	options = append(options, health.ProvideHealthCheck(func(db *sql.DB) health.NamedCheck {
		return health.NewNamedCheck("postgres", health.CheckFn(db.PingContext))
	}))

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
