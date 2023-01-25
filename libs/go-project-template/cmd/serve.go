package cmd

import (
	"context"
	"net/http"

	"github.com/formancehq/go-libs/health"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func newRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	// Plug middleware to handle traces
	r.Use(otelchi.Middleware(ServiceName))
	return r
}

func apiModule() fx.Option {
	return fx.Options(
		fx.Provide(newRouter),
		fx.Invoke(func(lc fx.Lifecycle, router *chi.Mux, healthController *health.HealthController) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					rootRouter := chi.NewRouter()
					rootRouter.Get("/_healthcheck", healthController.Check)
					rootRouter.Mount("/", router)
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
		// The module will expose a *health.HealthController
		// You must mount it on your api
		health.Module(),
		health.ProvideHealthCheck(func() health.NamedCheck {
			return health.NewNamedCheck("default", health.CheckFn(func(ctx context.Context) error {
				// TODO: Implements your own logic
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
			apiModule(),
			fx.NopLogger,
			// This will set up the telemetry stack
			// You have to add a middleware on your router to traces http requests
			otlptraces.CLITracesModule(viper.GetViper()),
		}

		app := fx.New(options...)
		err := app.Start(cmd.Context())
		if err != nil {
			return err
		}
		<-app.Done()
		return app.Err()
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
