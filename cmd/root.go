package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"numary.io/ledger/api"
	"numary.io/ledger/ledger"
)

var root = &cobra.Command{
	Use: "numary",
}

func Execute() {
	server := &cobra.Command{
		Use: "server",
	}

	server.AddCommand(&cobra.Command{
		Use: "start",
		Run: func(cmd *cobra.Command, args []string) {
			app := fx.New(
				fx.Provide(
					ledger.NewLedger,
					api.NewHttpAPI,
				),
				fx.Invoke(func(lc fx.Lifecycle, h *api.HttpAPI) {
				}),
			)

			app.Run()
		},
	})

	config := &cobra.Command{
		Use: "config",
	}

	config.AddCommand(&cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			c := DefaultConfig()
			b := c.Serialize()
			os.WriteFile("numary.config.json", []byte(b), 0644)
		},
	})

	root.AddCommand(server)
	root.AddCommand(config)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
