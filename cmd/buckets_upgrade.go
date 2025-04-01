package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlptraces"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

func NewBucketUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStorageDriver(cmd, func(driver *driver.Driver) error {
				if args[0] == "*" {
					return driver.UpgradeAllBuckets(cmd.Context())
				}

				return driver.UpgradeBucket(cmd.Context(), args[0])
			})
		},
	}

	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())

	return cmd
}

func withStorageDriver(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {

	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)

	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
	if err != nil {
		return err
	}

	var d *driver.Driver
	app := fx.New(
		fx.NopLogger,
		otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(Version)),
		otlptraces.FXModuleFromFlags(cmd),
		bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
		storage.NewFXModule(false),
		fx.Supply(fx.Annotate(logger, fx.As(new(logging.Logger)))),
		fx.Populate(&d),
	)
	err = app.Start(cmd.Context())
	if err != nil {
		return err
	}
	defer func() {
		_ = app.Stop(cmd.Context())
	}()

	return fn(d)
}
