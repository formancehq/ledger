package cmd

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/middlewares"
	"github.com/formancehq/stack/libs/go-libs/ballast"
	"github.com/formancehq/stack/libs/go-libs/httpserver"
	"github.com/formancehq/stack/libs/go-libs/logging"
	app "github.com/formancehq/stack/libs/go-libs/service"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	ballastSizeInBytesFlag = "ballast-size"
	numscriptCacheMaxCount = "numscript-cache-max-count"
)

func NewServe() *cobra.Command {
	cmd := &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(cmd.OutOrStdout(), resolveOptions(
				cmd.OutOrStdout(),
				ballast.Module(viper.GetUint(ballastSizeInBytesFlag)),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router, logger logging.Logger) {

					if viper.GetBool(app.DebugFlag) {
						wrappedRouter := chi.NewRouter()
						wrappedRouter.Use(func(handler http.Handler) http.Handler {
							return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
								handler.ServeHTTP(w, r)
							})
						})
						wrappedRouter.Use(middlewares.Log())
						wrappedRouter.Mount("/", h)
						h = wrappedRouter
					}

					lc.Append(httpserver.NewHook(viper.GetString(bindFlag), h))
				}),
			)...).Run(cmd.Context())
		},
	}
	cmd.Flags().Uint(ballastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Int(numscriptCacheMaxCount, 1024, "Numscript cache max count")
	return cmd
}
