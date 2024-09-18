package cmd

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/time"

	"github.com/formancehq/ledger/internal/storage/driver"

	"github.com/formancehq/ledger/internal/api"

	"github.com/formancehq/stack/libs/go-libs/ballast"
	"github.com/formancehq/stack/libs/go-libs/httpserver"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const (
	BallastSizeInBytesFlag     = "ballast-size"
	NumscriptCacheMaxCountFlag = "numscript-cache-max-count"
	ledgerBatchSizeFlag        = "ledger-batch-size"
	ReadOnlyFlag               = "read-only"
	AutoUpgradeFlag            = "auto-upgrade"
)

func NewServe() *cobra.Command {
	cmd := &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			readOnly, _ := cmd.Flags().GetBool(ReadOnlyFlag)
			autoUpgrade, _ := cmd.Flags().GetBool(AutoUpgradeFlag)
			ballastSize, _ := cmd.Flags().GetUint(BallastSizeInBytesFlag)
			bind, _ := cmd.Flags().GetString(BindFlag)

			return service.New(cmd.OutOrStdout(), resolveOptions(
				cmd,
				ballast.Module(ballastSize),
				api.Module(api.Config{
					Version:  Version,
					ReadOnly: readOnly,
					Debug:    service.IsDebug(cmd),
				}),
				fx.Invoke(func(lc fx.Lifecycle, driver *driver.Driver) {
					if autoUpgrade {
						lc.Append(fx.Hook{
							OnStart: driver.UpgradeAllBuckets,
						})
					}
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

					lc.Append(httpserver.NewHook(wrappedRouter, httpserver.WithAddress(bind)))
				}),
			)...).Run(cmd)
		},
	}
	cmd.Flags().Uint(BallastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Int(NumscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Int(ledgerBatchSizeFlag, 50, "ledger batch size")
	cmd.Flags().Bool(ReadOnlyFlag, false, "Read only mode")
	cmd.Flags().Bool(AutoUpgradeFlag, false, "Automatically upgrade all schemas")
	return cmd
}

func Log() func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			h.ServeHTTP(w, r)
			latency := time.Since(start)
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
