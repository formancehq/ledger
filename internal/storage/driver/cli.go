package driver

import (
	"context"
	"io"
	"time"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	storage "github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

// TODO(gfyrag): maybe move flag handling inside cmd/internal (as telemetry flags)
// Or make the inverse (move analytics flags to pkg/analytics)
// IMO, flags are more easily discoverable if located inside cmd/
func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Int(storage.StoreWorkerMaxPendingSize, 0, "Max pending size for store worker")
	cmd.PersistentFlags().Int(storage.StoreWorkerMaxWriteChanSize, 1024, "Max write channel size for store worker")
	cmd.PersistentFlags().String(storage.StoragePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgres connection string")
	cmd.PersistentFlags().Int(storage.StoragePostgresMaxIdleConnsFlag, 0, "Max idle connections to database")
	cmd.PersistentFlags().Duration(storage.StoragePostgresConnMaxIdleTimeFlag, time.Minute, "Max idle time of idle connections")
	cmd.PersistentFlags().Int(storage.StoragePostgresMaxOpenConns, 20, "Max open connections")
}

type PostgresConfig struct {
	ConnString string
}

func CLIModule(v *viper.Viper, output io.Writer, debug bool) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func(logger logging.Logger) sqlutils.ConnectionOptions {
		connectionOptions := storage.ConnectionOptionsFromFlags(v, output, debug)
		logger.WithField("config", connectionOptions).Infof("Opening connection to database...")
		return connectionOptions
	}))
	options = append(options, fx.Provide(func(connectionOptions sqlutils.ConnectionOptions) (*Driver, error) {
		return New(connectionOptions), nil
	}))

	options = append(options, fx.Invoke(func(driver *Driver, lifecycle fx.Lifecycle, logger logging.Logger) error {
		lifecycle.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				logger.Infof("Initializing database...")
				return driver.Initialize(ctx)
			},
			OnStop: func(ctx context.Context) error {
				logger.Infof("Closing driver...")
				return driver.Close()
			},
		})
		return nil
	}))
	return fx.Options(options...)
}
