package driver

import (
	"context"
	"io"
	"time"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

// TODO(gfyrag): maybe move flag handling inside cmd/internal (as telemetry flags)
// Or make the inverse (move analytics flags to pkg/analytics)
// IMO, flags are more easily discoverable if located inside cmd/
func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Int(storage.StoreWorkerMaxPendingSize, 0, "Max pending size for store worker")
	cmd.PersistentFlags().Int(storage.StoreWorkerMaxWriteChanSize, 1024, "Max write channel size for store worker")
	cmd.PersistentFlags().String(storage.StoragePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
	cmd.PersistentFlags().Int(storage.StoragePostgresMaxIdleConnsFlag, 20, "Max idle connections to database")
	cmd.PersistentFlags().Duration(storage.StoragePostgresConnMaxIdleTimeFlag, time.Minute, "Max idle time of idle connections")
	cmd.PersistentFlags().Int(storage.StoragePostgresMaxOpenConns, 20, "Max open connections")
}

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	PostgresConnectionOptions storage.ConnectionOptions
	Debug                     bool
}

func CLIModule(v *viper.Viper, output io.Writer, debug bool) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func() (*bun.DB, error) {
		return storage.OpenSQLDB(storage.ConnectionOptionsFromFlags(v, output, debug))
	}))
	options = append(options, fx.Provide(func(db *bun.DB) *storage.Database {
		return storage.NewDatabase(db)
	}))
	options = append(options, fx.Provide(func(db *storage.Database) (*Driver, error) {
		return New(db), nil
	}))
	options = append(options, health.ProvideHealthCheck(func(db *bun.DB) health.NamedCheck {
		return health.NewNamedCheck("postgres", health.CheckFn(db.PingContext))
	}))

	options = append(options, fx.Invoke(func(db *bun.DB, driver *Driver, lifecycle fx.Lifecycle) error {
		lifecycle.Append(fx.Hook{
			OnStart: driver.Initialize,
		})
		lifecycle.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				return db.Close()
			},
		})
		return nil
	}))
	return fx.Options(options...)
}
