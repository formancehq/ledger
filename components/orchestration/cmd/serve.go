package cmd

import (
	"context"
	"net/http"

	"github.com/formancehq/go-libs/health"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/orchestration/internal/api"
	"github.com/formancehq/orchestration/internal/storage"
	"github.com/formancehq/orchestration/internal/temporal"
	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func httpServerModule() fx.Option {
	return fx.Options(
		fx.Invoke(func(lc fx.Lifecycle, router *chi.Mux, healthController *health.HealthController) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					rootRouter := chi.NewRouter()
					rootRouter.Use(func(handler http.Handler) http.Handler {
						return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							w.Header().Set("Content-Type", "application/json")
							handler.ServeHTTP(w, r)
						})
					})
					rootRouter.Get("/_healthcheck", healthController.Check)
					rootRouter.Group(func(r chi.Router) {
						r.Use(otelchi.Middleware(ServiceName))
						r.Mount("/", router)
					})
					go func() {
						err := http.ListenAndServe(":8080", rootRouter)
						if err != nil {
							panic(err)
						}
					}()
					return nil
				},
			})
		}),
	)
}

func healthCheckModule() fx.Option {
	return fx.Options(
		health.Module(),
		health.ProvideHealthCheck(func() health.NamedCheck {
			return health.NewNamedCheck("default", health.CheckFn(func(ctx context.Context) error {
				return nil
			}))
		}),
	)
}

var serveCmd = &cobra.Command{
	Use: "serve",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return bindFlagsToViper(cmd)
	},
	RunE: func(cmd *cobra.Command, args []string) error {

		options := []fx.Option{
			healthCheckModule(),
			httpServerModule(),
			fx.NopLogger,
			// This will set up the telemetry stack
			// You have to add a middleware on your router to traces http requests
			otlptraces.CLITracesModule(viper.GetViper()),
			api.NewModule(),
			temporal.NewModule(
				viper.GetString(temporalAddressFlag),
				viper.GetString(temporalNamespaceFlag),
				viper.GetString(temporalSSLClientCertFlag),
				viper.GetString(temporalSSLClientKeyFlag),
			),
			storage.NewModule(viper.GetString(postgresDSNFlag), viper.GetBool(debugFlag)),
			workflow.NewModule(),
			fx.Invoke(func(lifecycle fx.Lifecycle, db *bun.DB) {
				lifecycle.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						return storage.Migrate(db, viper.GetBool(debugFlag))
					},
				})
			}),
		}

		app := fx.New(options...)
		err := app.Start(cmd.Context())
		if err != nil {
			return err
		}
		<-app.Done()
		return nil
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
