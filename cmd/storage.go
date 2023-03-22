package cmd

import (
	"context"
	"database/sql"
	"errors"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/system"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
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
				resolveOptions(viper.GetViper(), fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := viper.GetString("name")
							if name == "" {
								return errors.New("name is empty")
							}
							s, created, err := storageDriver.GetLedgerStore(ctx, name, true)
							if err != nil {
								return err
							}

							if !created {
								return nil
							}

							_, err = s.Initialize(ctx)
							if err != nil {
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
				resolveOptions(viper.GetViper(),
					fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
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
				resolveOptions(viper.GetViper(), fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := args[0]
							store, _, err := storageDriver.GetLedgerStore(ctx, name, false)
							if err != nil {
								return err
							}
							modified, err := store.Initialize(ctx)
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

func NewStorageScan() *cobra.Command {
	cmd := &cobra.Command{
		Use: "scan",
		RunE: func(cmd *cobra.Command, args []string) error {

			opt := fx.Invoke(func(driver *sqlstorage.Driver, sqlDb *bun.DB, db schema.DB, lc fx.Lifecycle) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						rows, err := sqlDb.QueryContext(ctx, `
								SELECT s.schema_name
								FROM information_schema.schemata s
								JOIN pg_catalog.pg_tables t ON t.schemaname = s.schema_name AND t.tablename = 'transactions'
							`)
						if err != nil {
							return err
						}
						defer func(rows *sql.Rows) {
							if err := rows.Close(); err != nil {
								panic(err)
							}
						}(rows)
						for rows.Next() {
							var ledgerName string
							err := rows.Scan(&ledgerName)
							if err != nil {
								return err
							}

							if ledgerName == sqlstorage.SystemSchema {
								continue
							}
							// This command is dedicated to upgrade ledger version before 1.4
							// It will be removed in a near future, so we can assert the system store type without risk
							created, err := driver.GetSystemStore().(*system.Store).
								Register(cmd.Context(), ledgerName)
							if err != nil {
								continue
							}
							if created {
								logging.FromContext(ctx).Infof("Ledger '%s' registered", ledgerName)
							} else {
								logging.FromContext(ctx).Infof("Ledger '%s' already registered", ledgerName)
							}
						}

						return nil
					},
				})
			})

			app := service.New(cmd.OutOrStdout(), resolveOptions(viper.GetViper(), opt)...)
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
				resolveOptions(viper.GetViper(), fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := args[0]
							store, _, err := storageDriver.GetLedgerStore(ctx, name, false)
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
