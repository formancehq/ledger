package api

import (
	_ "embed"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/audit/httpaudit"
	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/fx/servicefx"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/controller/system"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
)

type BulkConfig struct {
	MaxSize  int
	Parallel int
}

type AuditConfig struct {
	Enabled            bool
	AsyncEnabled       bool
	AsyncQueueCapacity int
	AsyncWorkerCount   int
}

type Config struct {
	Version              string
	Debug                bool
	Bulk                 BulkConfig
	Pagination           storagecommon.PaginationConfig
	Exporters            bool
	ExperimentalFeatures []string
	Audit                AuditConfig
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(
			lc fx.Lifecycle,
			backend system.Controller,
			authenticator jwt.Authenticator,
			publisher message.Publisher,
			tracerProvider trace.TracerProvider,
		) chi.Router {
			auditOptions := []httpaudit.HTTPOption{
				httpaudit.WithEnabled(cfg.Audit.Enabled),
			}
			if cfg.Audit.Enabled && cfg.Audit.AsyncEnabled {
				asyncPublisher := httpaudit.NewAsyncPublisher(
					publisher,
					"audit-events",
					"ledger",
					httpaudit.WithAsyncPublishingQueueCapacity(cfg.Audit.AsyncQueueCapacity),
					httpaudit.WithAsyncPublishingWorkerCount(cfg.Audit.AsyncWorkerCount),
				)
				lc.Append(fx.Hook{
					OnStop: asyncPublisher.Close,
				})
				auditOptions = append(auditOptions, httpaudit.WithAsyncPublishing(asyncPublisher))
			}

			return NewRouter(
				backend,
				authenticator,
				publisher,
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
				WithExperimentalFeatures(cfg.ExperimentalFeatures),
				WithAuditHTTPOptions(auditOptions...),
			)
		}),
		servicefx.HealthModule(),
	)
}
