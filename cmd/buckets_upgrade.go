package cmd

import (
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/service"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucketUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
			if err != nil {
				return err
			}
			defer func() {
				_ = db.Close()
			}()

			driver := driver.New(db)
			if err := driver.Initialize(cmd.Context()); err != nil {
				return err
			}

			if args[0] == "*" {
				return upgradeAll(cmd)
			}

			logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false)

			return driver.UpgradeBucket(logging.ContextWithLogger(cmd.Context(), logger), args[0])
		},
	}

	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())

	return cmd
}

func upgradeAll(cmd *cobra.Command) error {
	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false)
	ctx := logging.ContextWithLogger(cmd.Context(), logger)

	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
	if err != nil {
		return err
	}

	db, err := bunconnect.OpenSQLDB(cmd.Context(), *connectionOptions)
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	driver := driver.New(db)
	if err := driver.Initialize(ctx); err != nil {
		return err
	}

	return driver.UpgradeAllBuckets(ctx)
}
