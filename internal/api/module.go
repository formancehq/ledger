package api

import (
	_ "embed"
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
	)
}
