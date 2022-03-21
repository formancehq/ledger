package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/numary/ledger/pkg/storage"
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
							name, err := cmd.Flags().GetString("name")
							if err != nil {
								return err
							}
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
