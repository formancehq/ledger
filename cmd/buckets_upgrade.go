package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/fx/observefx"
	"github.com/formancehq/go-libs/v5/pkg/fx/storagefx"
	"github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/service"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/driver"
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
	connect.AddFlags(cmd.Flags())

	return cmd
}

func withStorageDriver(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {

	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)

	connectionOptions, err := connect.ConnectionOptionsFromFlags(cmd.Flags(), cmd.Context())
	if err != nil {
		return err
	}

	var d *driver.Driver
	app := fx.New(
		fx.NopLogger,
		observefx.ResourceModuleFromFlags(cmd, observe.WithServiceVersion(Version)),
		observefx.TracesModuleFromFlags(cmd),
		storagefx.BunConnectModule(*connectionOptions, service.IsDebug(cmd)),
		storage.NewFXModule(storage.ModuleConfig{}),
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
