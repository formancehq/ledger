package cmd

import (
	"net/http"
	"time"

	"github.com/formancehq/ledger/internal/api"

	ledger "github.com/formancehq/ledger/internal"
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
	ballastSizeInBytesFlag     = "ballast-size"
	numscriptCacheMaxCountFlag = "numscript-cache-max-count"
	readOnlyFlag               = "read-only"
)

func NewServe() *cobra.Command {
	cmd := &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(cmd.OutOrStdout(), resolveOptions(
				cmd.OutOrStdout(),
				ballast.Module(viper.GetUint(ballastSizeInBytesFlag)),
				api.Module(api.Config{
					Version:  Version,
					ReadOnly: viper.GetBool(readOnlyFlag),
				}),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router, logger logging.Logger) {

					wrappedRouter := chi.NewRouter()
					wrappedRouter.Use(func(handler http.Handler) http.Handler {
						return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
							handler.ServeHTTP(w, r)
						})
					})
					wrappedRouter.Use(Log())
					wrappedRouter.Mount("/", h)

					lc.Append(httpserver.NewHook(viper.GetString(bindFlag), wrappedRouter))
				}),
			)...).Run(cmd.Context())
		},
	}
	cmd.Flags().Uint(ballastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Int(numscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Bool(readOnlyFlag, false, "Read only mode")
	return cmd
}

func Log() func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := ledger.Now()
			h.ServeHTTP(w, r)
			latency := time.Since(start.Time)
			logging.FromContext(r.Context()).WithFields(map[string]interface{}{
				"method":     r.Method,
				"path":       r.URL.Path,
				"latency":    latency,
				"user_agent": r.UserAgent(),
				"params":     r.URL.Query().Encode(),
			}).Debug("Request")
		})
	}
}
