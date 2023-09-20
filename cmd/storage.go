package cmd

import (
	"context"
	"errors"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
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

func upgradeStore(ctx context.Context, store *ledgerstore.Store, name string) error {
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
}

func NewStorageUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {

			sqlDB, err := storage.OpenSQLDB(storage.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag)))
			if err != nil {
				return err
			}
			defer sqlDB.Close()

			driver := driver.New(sqlDB)
			if err := driver.Initialize(cmd.Context()); err != nil {
				return err
			}

			name := args[0]
			store, err := driver.GetLedgerStore(cmd.Context(), name)
			if err != nil {
				return err
			}
			logger := service.GetDefaultLogger(cmd.OutOrStdout(), viper.GetBool(service.DebugFlag), false)

			return upgradeStore(logging.ContextWithLogger(cmd.Context(), logger), store, name)
		},
	}
	return cmd
}

func NewStorageUpgradeAll() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade-all",
		Args:         cobra.ExactArgs(0),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {

			logger := service.GetDefaultLogger(cmd.OutOrStdout(), viper.GetBool(service.DebugFlag), false)
			ctx := logging.ContextWithLogger(cmd.Context(), logger)

			sqlDB, err := storage.OpenSQLDB(storage.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag)))
			if err != nil {
				return err
			}
			defer sqlDB.Close()

			driver := driver.New(sqlDB)
			if err := driver.Initialize(ctx); err != nil {
				return err
			}

			systemStore := driver.GetSystemStore()
			ledgers, err := systemStore.ListLedgers(ctx)
			if err != nil {
				return err
			}

			for _, ledger := range ledgers {
				store, err := driver.GetLedgerStore(ctx, ledger)
				if err != nil {
					return err
				}
				logger.Infof("Upgrading storage '%s'", ledger)
				if err := upgradeStore(ctx, store, ledger); err != nil {
					return err
				}
			}

			return nil
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
