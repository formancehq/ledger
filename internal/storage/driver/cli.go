package driver

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	"github.com/spf13/cobra"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"go.uber.org/fx"
)

type PostgresConfig struct {
	ConnString string
}

func CLIModule(cmd *cobra.Command) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options, fx.Provide(func() (*bunconnect.ConnectionOptions, error) {
		return bunconnect.ConnectionOptionsFromFlags(cmd.Context())
	}))
	options = append(options, fx.Provide(func(connectionOptions *bunconnect.ConnectionOptions) (*Driver, error) {
		return New(*connectionOptions), nil
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
