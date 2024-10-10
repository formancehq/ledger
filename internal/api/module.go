package api

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/go-libs/httpserver"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/health"
	"go.uber.org/fx"
)

type Config struct {
	Version string
	Debug   bool
	Bind    string
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			backend system.Controller,
			healthController *health.HealthController,
			authenticator auth.Authenticator,
			logger logging.Logger,
		) chi.Router {
			return NewRouter(
				backend,
				healthController,
				authenticator,
				logger,
				"develop",
				cfg.Debug,
			)
		}),
		health.Module(),
		fx.Invoke(func(lc fx.Lifecycle, h chi.Router, logger logging.Logger) {

			wrappedRouter := chi.NewRouter()
			wrappedRouter.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					r = r.WithContext(logging.ContextWithLogger(r.Context(), logger))
					handler.ServeHTTP(w, r)
				})
			})
			wrappedRouter.Mount("/", h)

			lc.Append(httpserver.NewHook(wrappedRouter, httpserver.WithAddress(cfg.Bind)))
		}),
	)
}
