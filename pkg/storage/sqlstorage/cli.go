package sqlstorage

import (
	"database/sql"

	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/worker"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"go.uber.org/fx"
)

const (
	StoreWorkerMaxPendingSize           = "store-worker-max-pending-size"
	StoreWorkerMaxWriteChanSize         = "store-worker-max-write-chan-size"
	StoragePostgresConnectionStringFlag = "storage-postgres-conn-string"
)

func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Int(StoreWorkerMaxPendingSize, 0, "Max pending size for store worker")
	cmd.PersistentFlags().Int(StoreWorkerMaxWriteChanSize, 1024, "Max write channel size for store worker")
	cmd.PersistentFlags().String(StoragePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
}

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	PostgresConfig *PostgresConfig
	StoreConfig    ledgerstore.StoreConfig
}

func OpenSQLDB(dataSourceName string) (*bun.DB, error) {
	sqldb, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}

	db := bun.NewDB(sqldb, pgdialect.New())

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func CLIDriverModule(v *viper.Viper) fx.Option {
	cfg := ModuleConfig{
		PostgresConfig: &PostgresConfig{
			ConnString: v.GetString(StoragePostgresConnectionStringFlag),
		},
		StoreConfig: ledgerstore.StoreConfig{
			StoreWorkerConfig: worker.WorkerConfig{
				MaxPendingSize:   v.GetInt(StoreWorkerMaxPendingSize),
				MaxWriteChanSize: v.GetInt(StoreWorkerMaxWriteChanSize),
			},
		},
	}

	options := make([]fx.Option, 0)

	options = append(options, fx.Provide(func() (*bun.DB, error) {
		return OpenSQLDB(cfg.PostgresConfig.ConnString)
	}))
	options = append(options, fx.Provide(func(db *bun.DB) schema.DB {
		return schema.NewPostgresDB(db)
	}))
	options = append(options, fx.Provide(func(db schema.DB) (*Driver, error) {
		return NewDriver("postgres", db, cfg.StoreConfig), nil
	}))
	options = append(options, health.ProvideHealthCheck(func(db *bun.DB) health.NamedCheck {
		return health.NewNamedCheck("postgres", health.CheckFn(db.PingContext))
	}))

	options = append(options, fx.Provide(func(driver *Driver) storage.Driver {
		return driver
	}))
	options = append(options, fx.Invoke(func(driver storage.Driver, lifecycle fx.Lifecycle) error {
		lifecycle.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop:  driver.Close,
		})
		return nil
	}))
	return fx.Options(options...)
}
