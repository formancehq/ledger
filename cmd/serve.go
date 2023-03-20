package cmd

import (
	"github.com/formancehq/stack/libs/go-libs/httpserver"
	app "github.com/formancehq/stack/libs/go-libs/service"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func NewServe() *cobra.Command {
	return &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(cmd.OutOrStdout(), resolveOptions(
				viper.GetViper(),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router) {
					lc.Append(httpserver.NewHook(viper.GetString(bindFlag), h))
				}),
			)...).Run(cmd.Context())
		},
	}
}
