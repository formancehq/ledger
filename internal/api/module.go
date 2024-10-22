package api

import (
	_ "embed"
	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/go-libs/v2/health"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type Config struct {
	Version     string
	Debug       bool
	BulkMaxSize int
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			backend system.Controller,
			authenticator auth.Authenticator,
			logger logging.Logger,
			tracer trace.TracerProvider,
		) chi.Router {
			return NewRouter(
				backend,
				authenticator,
				logger,
				"develop",
				cfg.Debug,
				WithTracer(tracer.Tracer("api")),
				WithBulkMaxSize(cfg.BulkMaxSize),
			)
		}),
		health.Module(),
	)
}
