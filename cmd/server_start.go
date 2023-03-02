package cmd

import (
	"github.com/formancehq/stack/libs/go-libs/httpserver"
	app "github.com/formancehq/stack/libs/go-libs/service"
	"github.com/numary/ledger/pkg/api"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func NewServerStart() *cobra.Command {
	return &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(cmd.OutOrStdout(), resolveOptions(
				viper.GetViper(),
				fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
					lc.Append(httpserver.NewHook(viper.GetString(bindFlag), h))
				}),
			)...).Run(cmd.Context())
		},
	}
}
