package cmd

import (
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag))
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

			bucket, err := driver.OpenBucket(name)
			if err != nil {
				return err
			}

			logger := service.GetDefaultLogger(cmd.OutOrStdout(), viper.GetBool(service.DebugFlag), false)

			return bucket.Migrate(logging.ContextWithLogger(cmd.Context(), logger))
		},
	}
	return cmd
}

func NewBucketUpgradeAll() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade-all",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {

			logger := service.GetDefaultLogger(cmd.OutOrStdout(), viper.GetBool(service.DebugFlag), false)
			ctx := logging.ContextWithLogger(cmd.Context(), logger)

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag))
			if err != nil {
				return err
			}

			driver := driver.New(*connectionOptions)
			defer func() {
				_ = driver.Close()
			}()

			if err := driver.Initialize(ctx); err != nil {
				return err
			}

			return driver.UpgradeAllBuckets(ctx)
		},
	}
	return cmd
}
