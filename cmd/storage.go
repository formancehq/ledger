package cmd

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
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
			app := NewContainer(
				viper.GetViper(),
				fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := viper.GetString("name")
							if name == "" {
								return errors.New("name is empty")
							}
							fmt.Printf("Creating ledger '%s'...", name)
							s, created, err := storageDriver.GetStore(ctx, name, true)
							if err != nil {
								return err
							}

							if !created {
								fmt.Printf("Already initialized!\r\n")
								return nil
							}

							_, err = s.Initialize(ctx)
							if err != nil {
								return err
							}
							fmt.Printf(" OK\r\n")
							return nil
						},
					})
				}),
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
			app := NewContainer(
				viper.GetViper(),
				fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							ledgers, err := storageDriver.List(ctx)
							if err != nil {
								return err
							}
							if len(ledgers) == 0 {
								fmt.Println("No ledger found.")
								return nil
							}
							fmt.Println("Ledgers:")
							for _, l := range ledgers {
								fmt.Println("- " + l)
							}
							return nil
						},
					})
				}),
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
			app := NewContainer(
				viper.GetViper(),
				fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := args[0]
							store, _, err := storageDriver.GetStore(ctx, name, false)
							if err != nil {
								return err
							}
							modified, err := store.Initialize(ctx)
							if err != nil {
								return err
							}
							if modified {
								fmt.Printf("Storage '%s' migrated\r\n", name)
							} else {
								fmt.Printf("Storage '%s' left in place\r\n", name)
							}
							return nil
						},
					})
				}),
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

			var opt fx.Option

			switch viper.GetString(storageDriverFlag) {
			default:
				return errors.New("Invalid storage driver: " + viper.GetString(storageDriverFlag))
			case "postgres":
				opt = fx.Invoke(func(driver *sqlstorage.Driver, sqlDb *sql.DB, db sqlstorage.DB, lc fx.Lifecycle) {
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
								fmt.Printf("Registering ledger '%s'\r\n", ledgerName)
								created, err := driver.Register(cmd.Context(), ledgerName)
								if err != nil {
									fmt.Printf("Error registering ledger '%s': %s\r\n", ledgerName, err)
									continue
								}
								if created {
									fmt.Printf("Ledger '%s' registered\r\n", ledgerName)
								} else {
									fmt.Printf("Ledger '%s' already registered\r\n", ledgerName)
								}
							}

							return nil
						},
					})
				})
			case "sqlite":
				opt = fx.Invoke(func(driver *sqlstorage.Driver, db sqlstorage.DB, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							files, err := os.ReadDir(viper.GetString(storageDirFlag))
							if err != nil {
								return err
							}
							for _, f := range files {
								if !strings.HasSuffix(f.Name(), ".db") {
									fmt.Println("Skip file " + f.Name())
									continue
								}
								f := strings.TrimSuffix(f.Name(), ".db")
								parts := strings.SplitN(f, "_", 2)
								if len(parts) != 2 {
									fmt.Println("Skip file " + f + ".db : Bad name")
									continue
								}
								if parts[0] != viper.GetString(storageSQLiteDBNameFlag) {
									fmt.Println("Skip file " + f + ".db : DB name not mathing")
									continue
								}
								ledgerName := parts[1]
								if ledgerName == sqlstorage.SystemSchema {
									continue
								}
								fmt.Printf("Registering ledger '%s'\r\n", ledgerName)
								created, err := driver.Register(cmd.Context(), ledgerName)
								if err != nil {
									fmt.Printf("Error registering ledger '%s': %s\r\n", ledgerName, err)
									continue
								}
								if created {
									fmt.Printf("Ledger '%s' registered\r\n", ledgerName)
								} else {
									fmt.Printf("Ledger '%s' already registered\r\n", ledgerName)
								}
							}
							return nil
						},
					})
				})
			}

			app := NewContainer(viper.GetViper(), opt)
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
			app := NewContainer(
				viper.GetViper(),
				fx.Invoke(func(storageDriver storage.Driver, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							name := args[0]
							err := storageDriver.DeleteStore(ctx, name)
							if err != nil {
								return err
							}
							fmt.Println("Storage deleted!")
							return nil
						},
					})
				}),
			)
			return app.Start(cmd.Context())
		},
	}
	return cmd
}
