package cmd

import (
	"context"
	"github.com/numary/ledger/pkg/storage"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

func NewStorageInit() *cobra.Command {
	return &cobra.Command{
		Use: "init",
		RunE: func(cmd *cobra.Command, args []string) error {
			NewContainer(
				fx.Invoke(func(storageFactory storage.Factory) error {
					s, err := storageFactory.GetStore("default")
					if err != nil {
						return err
					}

					err = s.Initialize(context.Background())
					if err != nil {
						return err
					}
					return nil
				}),
			)
			return nil
		},
	}
}
