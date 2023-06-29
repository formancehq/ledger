package cmd

import (
	"context"
	"errors"

	"github.com/formancehq/ledger/pkg/storage/driver"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func NewStorage() *cobra.Command {
	return &cobra.Command{
		Use: "storage",
	}
}

func NewStorageInit() *cobra.Command {
	cmd := &cobra.Command{
		Use: "init",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := service.New(
				cmd.OutOrStdout(),
				resolveOptions(
					cmd.OutOrStdout(),
					fx.Invoke(func(storageDriver *driver.Driver, lc fx.Lifecycle) {
						lc.Append(fx.Hook{
							OnStart: func(ctx context.Context) error {
								name := viper.GetString("name")
								if name == "" {
									return errors.New("name is empty")
								}

								exists, err := storageDriver.GetSystemStore().Exists(ctx, name)
								if err != nil {
									return err
								}

								if exists {
									return errors.New("ledger already exists")
								}

								store, err := storageDriver.CreateLedgerStore(ctx, name)
								if err != nil {
									return err
								}

								_, err = store.Migrate(ctx)
								return err
							},
						})
					}))...,
			)
			return app.Start(cmd.Context())
		},
	}
	cmd.Flags().String("name", "default", "Ledger name")
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		panic(err)
	}
	return cmd
}

func NewStorageList() *cobra.Command {
	cmd := &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := service.New(cmd.OutOrStdout(),
				resolveOptions(
					cmd.OutOrStdout(),
					fx.Invoke(func(storageDriver *driver.Driver, lc fx.Lifecycle) {
						lc.Append(fx.Hook{
							OnStart: func(ctx context.Context) error {
								ledgers, err := storageDriver.GetSystemStore().ListLedgers(ctx)
								if err != nil {
									return err
								}
								if len(ledgers) == 0 {
									logging.FromContext(ctx).Info("No ledger found.")
									return nil
								}
								logging.FromContext(ctx).Infof("Ledgers: %v", ledgers)
								return nil
							},
						})
					}),
				)...,
			)
			return app.Start(cmd.Context())
		},
	}
	return cmd
}

func NewStorageUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "upgrade",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := service.New(cmd.OutOrStdout(),
				resolveOptions(
					cmd.OutOrStdout(),
					fx.Invoke(func(storageDriver *driver.Driver, lc fx.Lifecycle) {
						lc.Append(fx.Hook{
							OnStart: func(ctx context.Context) error {
								name := args[0]
								store, err := storageDriver.GetLedgerStore(ctx, name)
								if err != nil {
									return err
								}
								modified, err := store.Migrate(ctx)
								if err != nil {
									return err
								}
								if modified {
									logging.FromContext(ctx).Infof("Storage '%s' upgraded", name)
								} else {
									logging.FromContext(ctx).Infof("Storage '%s' is up to date", name)
								}
								return nil
							},
						})
					}))...,
			)
			return app.Start(cmd.Context())
		},
	}
	return cmd
}

func NewStorageDelete() *cobra.Command {
	cmd := &cobra.Command{
		Use:  "delete",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := service.New(
				cmd.OutOrStdout(),
				resolveOptions(
					cmd.OutOrStdout(),
					fx.Invoke(func(storageDriver *driver.Driver, lc fx.Lifecycle) {
						lc.Append(fx.Hook{
							OnStart: func(ctx context.Context) error {
								name := args[0]
								store, err := storageDriver.GetLedgerStore(ctx, name)
								if err != nil {
									return err
								}
								if err := store.Delete(ctx); err != nil {
									return err
								}
								return nil
							},
						})
					}))...,
			)
			return app.Start(cmd.Context())
		},
	}
	return cmd
}
