package cmd

import (
	"fmt"
	"os"

	"github.com/numary/ledger/api"
	"github.com/numary/ledger/config"
	"github.com/numary/ledger/ledger"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
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
					config.GetConfig,
					ledger.NewLedger,
					api.NewHttpAPI,
				),
				fx.Invoke(func(lc fx.Lifecycle, h *api.HttpAPI) {
				}),
			)

			app.Run()
		},
	})

	conf := &cobra.Command{
		Use: "config",
	}

	conf.AddCommand(&cobra.Command{
		Use: "init",
		Run: func(cmd *cobra.Command, args []string) {
			c := config.DefaultConfig()
			b := c.Serialize()
			os.WriteFile("numary.config.json", []byte(b), 0644)
		},
	})

	root.AddCommand(server)
	root.AddCommand(conf)
	root.AddCommand(UICmd)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
