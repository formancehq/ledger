package driver

import (
	"context"
	"io"
	"time"

	storage2 "github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

// TODO(gfyrag): maybe move flag handling inside cmd/internal (as telemetry flags)
// Or make the inverse (move analytics flags to pkg/analytics)
// IMO, flags are more easily discoverable if located inside cmd/
func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Int(storage2.StoreWorkerMaxPendingSize, 0, "Max pending size for store worker")
	cmd.PersistentFlags().Int(storage2.StoreWorkerMaxWriteChanSize, 1024, "Max write channel size for store worker")
	cmd.PersistentFlags().String(storage2.StoragePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
	cmd.PersistentFlags().Int(storage2.StoragePostgresMaxIdleConnsFlag, 20, "Max idle connections to database")
	cmd.PersistentFlags().Duration(storage2.StoragePostgresConnMaxIdleTimeFlag, time.Minute, "Max idle time of idle connections")
	cmd.PersistentFlags().Int(storage2.StoragePostgresMaxOpenConns, 20, "Max open connections")
}

type PostgresConfig struct {
	ConnString string
}

type ModuleConfig struct {
	PostgresConnectionOptions storage2.ConnectionOptions
	Debug                     bool
}

func CLIModule(v *viper.Viper, output io.Writer, debug bool) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func(logger logging.Logger) (*bun.DB, error) {
		configuration := storage2.ConnectionOptionsFromFlags(v, output, debug)
		logger.WithField("config", configuration).Infof("Opening connection to database...")
		return storage2.OpenSQLDB(configuration)
	}))
	options = append(options, fx.Provide(func(db *bun.DB) (*Driver, error) {
		return New(db), nil
	}))
	options = append(options, health.ProvideHealthCheck(func(db *bun.DB) health.NamedCheck {
		return health.NewNamedCheck("postgres", health.CheckFn(db.PingContext))
	}))

	options = append(options, fx.Invoke(func(db *bun.DB, driver *Driver, lifecycle fx.Lifecycle, logger logging.Logger) error {
		lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				logger.Infof("Initializing database...")
				return driver.Initialize(ctx)
			},
			OnStop: func(ctx context.Context) error {
				logger.Infof("Closing database...")
				return db.Close()
			},
		})
		return nil
	}))
	return fx.Options(options...)
}
