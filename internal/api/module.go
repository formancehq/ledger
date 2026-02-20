package api

import (
	_ "embed"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v4/auth"
	"github.com/formancehq/go-libs/v4/health"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

type BulkConfig struct {
	MaxSize  int
	Parallel int
}

type Config struct {
	Version    string
	Debug      bool
	Bulk       BulkConfig
	Pagination storagecommon.PaginationConfig
	Exporters  bool
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
				WithExporters(cfg.Exporters),
			)
		}),
		health.Module(),
	)
}
