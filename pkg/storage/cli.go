package storage

import (
	"io"
	"time"

	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/ledger/pkg/storage/schema"
	"github.com/formancehq/ledger/pkg/storage/utils"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

const (
	StoreWorkerMaxPendingSize           = "store-worker-max-pending-size"
	StoreWorkerMaxWriteChanSize         = "store-worker-max-write-chan-size"
	StoragePostgresConnectionStringFlag = "storage-postgres-conn-string"
	StoragePostgresMaxIdleConnsFlag     = "storage-postgres-max-idle-conns"
	StoragePostgresConnMaxIdleTimeFlag  = "storage-postgres-conn-max-idle-time"
	StoragePostgresMaxOpenConns         = "storage-postgres-max-open-conns"
)

// TODO(gfyrag): maybe move flag handling inside cmd/internal (as telemetry flags)
// Or make the inverse (move analytics flags to pkg/analytics)
// IMO, flags are more easily discoverable if located inside cmd/
func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Int(StoreWorkerMaxPendingSize, 0, "Max pending size for store worker")
	cmd.PersistentFlags().Int(StoreWorkerMaxWriteChanSize, 1024, "Max write channel size for store worker")
	cmd.PersistentFlags().String(StoragePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
	cmd.PersistentFlags().Int(StoragePostgresMaxIdleConnsFlag, 20, "Max idle connections to database")
	cmd.PersistentFlags().Duration(StoragePostgresConnMaxIdleTimeFlag, time.Minute, "Max idle time of idle connections")
	cmd.PersistentFlags().Int(StoragePostgresMaxOpenConns, 20, "Max open connections")
}

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	PostgresConnectionOptions utils.ConnectionOptions
	StoreConfig               ledgerstore.StoreConfig
	Debug                     bool
}

func CLIDriverModule(v *viper.Viper, output io.Writer, debug bool) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func() (*bun.DB, error) {
		return utils.OpenSQLDB(utils.ConnectionOptions{
			DatabaseSourceName: v.GetString(StoragePostgresConnectionStringFlag),
			Debug:              debug,
			Writer:             output,
			MaxIdleConns:       v.GetInt(StoragePostgresMaxIdleConnsFlag),
			ConnMaxIdleTime:    v.GetDuration(StoragePostgresConnMaxIdleTimeFlag),
			MaxOpenConns:       v.GetInt(StoragePostgresMaxOpenConns),
		})
	}))
	options = append(options, fx.Provide(func(db *bun.DB) schema.DB {
		return schema.NewPostgresDB(db)
	}))
	options = append(options, fx.Provide(func(db schema.DB) (*Driver, error) {
		return NewDriver("postgres", db, ledgerstore.StoreConfig{
			StoreWorkerConfig: ledgerstore.Config{
				MaxPendingSize:   v.GetInt(StoreWorkerMaxPendingSize),
				MaxWriteChanSize: v.GetInt(StoreWorkerMaxWriteChanSize),
			},
		}), nil
	}))
	options = append(options, health.ProvideHealthCheck(func(db *bun.DB) health.NamedCheck {
		return health.NewNamedCheck("postgres", health.CheckFn(db.PingContext))
	}))

	options = append(options, fx.Invoke(func(driver *Driver, lifecycle fx.Lifecycle) error {
		lifecycle.Append(fx.Hook{
			OnStart: driver.Initialize,
			OnStop:  driver.Close,
		})
		return nil
	}))
	return fx.Options(options...)
}
