package api

import (
	_ "embed"
	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type BulkConfig struct {
	MaxSize  int
	Parallel int
}

type Config struct {
	Version    string
	Debug      bool
	Bulk       BulkConfig
	Pagination common.PaginationConfig
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			backend system.Controller,
			authenticator auth.Authenticator,
			tracerProvider trace.TracerProvider,
		) chi.Router {
			return NewRouter(
				backend,
				authenticator,
				cfg.Version,
				cfg.Debug,
				WithTracer(tracerProvider.Tracer("api")),
				WithBulkMaxSize(cfg.Bulk.MaxSize),
				WithBulkerFactory(bulking.NewDefaultBulkerFactory(
					bulking.WithParallelism(cfg.Bulk.Parallel),
					bulking.WithTracer(tracerProvider.Tracer("api.bulking")),
				)),
				WithPaginationConfiguration(cfg.Pagination),
			)
		}),
		health.Module(),
	)
}
