package cmd

import (
	"github.com/formancehq/go-libs/bun/bunconnect"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/service"
	"github.com/formancehq/ledger/v2/internal/storage/driver"
	"github.com/spf13/cobra"
)

func NewBucket() *cobra.Command {
	return &cobra.Command{
		Use:     "buckets",
		Aliases: []string{"storage"},
	}
}

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

			driver := driver.New(*connectionOptions)
			if err := driver.Initialize(cmd.Context()); err != nil {
				return err
			}
			defer func() {
				_ = driver.Close()
			}()

			name := args[0]

			bucket, err := driver.OpenBucket(cmd.Context(), name)
			if err != nil {
				return err
			}

			logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false)

			return bucket.Migrate(logging.ContextWithLogger(cmd.Context(), logger))
		},
	}
	return cmd
}

func upgradeAll(cmd *cobra.Command, _ []string) error {
	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false)
	ctx := logging.ContextWithLogger(cmd.Context(), logger)

	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
	if err != nil {
		return err
	}

	driver := driver.New(*connectionOptions)
	if err := driver.Initialize(ctx); err != nil {
		return err
	}
	defer func() {
		_ = driver.Close()
	}()

	return driver.UpgradeAllBuckets(ctx)
}
