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
							s, created, err := storageDriver.NewStore(ctx, name)
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
	cmd.Flags().String("name", "default", "Ledger name")
	return cmd
}
