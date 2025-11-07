package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/api/bulking"
	"github.com/formancehq/ledger/internal/api/common"
	v1 "github.com/formancehq/ledger/internal/api/v1"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

// NewRouter creates a chi.Router configured with the v2 HTTP API routes for the ledger service.
// It registers authentication-protected top-level endpoints (including /_info), an "/_" group
// that may expose exporter management and bucket operations, ledger-scoped routes (ledger creation,
// metadata, and nested ledger subroutes such as bulk operations, info, stats, pipelines when
// enabled, logs, accounts, transactions, aggregated balances, and volumes), and applies tracing
// attributes for the selected ledger on ledger-scoped requests.
// The behavior of tracing, bulking, bulk handler factories, pagination, and whether exporter-related
// endpoints are mounted is controlled via RouterOption arguments.
func NewRouter(
	systemController systemcontroller.Controller,
	authenticator auth.Authenticator,
	version string,
	opts ...RouterOption,
) chi.Router {
	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	router := chi.NewMux()

	router.Group(func(router chi.Router) {
		router.Use(auth.Middleware(authenticator))

		router.Get("/_info", v1.GetInfo(systemController, version))

		router.Route("/_", func(router chi.Router) {
			if routerOptions.exporters {
				router.Route("/exporters", func(router chi.Router) {
					router.Get("/", listExporters(systemController))
					router.Get("/{exporterID}", getExporter(systemController))
					router.Put("/{exporterID}", updateExporter(systemController))
					router.Delete("/{exporterID}", deleteExporter(systemController))
					router.Post("/", createExporter(systemController))
				})
			}
			router.Route("/buckets", func(router chi.Router) {
				router.Delete("/{bucket}", deleteBucket(systemController))
				router.Post("/{bucket}/restore", restoreBucket(systemController))
			})
		})
		router.Get("/", listLedgers(systemController, routerOptions.paginationConfig))
		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					trace.
						SpanFromContext(r.Context()).
						SetAttributes(attribute.String("ledger", chi.URLParam(r, "ledger")))
					handler.ServeHTTP(w, r)
				})
			})
			router.Post("/", createLedger(systemController))
			router.Get("/", readLedger(systemController))
			router.Put("/metadata", updateLedgerMetadata(systemController))
			router.Delete("/metadata/{key}", deleteLedgerMetadata(systemController))

			router.With(common.LedgerMiddleware(systemController, func(r *http.Request) string {
				return chi.URLParam(r, "ledger")
			}, routerOptions.tracer, "/_info")).Group(func(router chi.Router) {
				router.Post("/_bulk", bulkHandler(
					routerOptions.bulkerFactory,
					routerOptions.bulkHandlerFactories,
				))

				router.Get("/_info", getLedgerInfo)
				router.Get("/stats", readStats)

				if routerOptions.exporters {
					router.Route("/pipelines", func(router chi.Router) {
						router.Get("/", listPipelines(systemController))
						router.Post("/", createPipeline(systemController))
						router.Route("/{pipelineID}", func(router chi.Router) {
							router.Get("/", readPipeline(systemController))
							router.Delete("/", deletePipeline(systemController))
							router.Post("/start", startPipeline(systemController))
							router.Post("/stop", stopPipeline(systemController))
							router.Post("/reset", resetPipeline(systemController))
						})
					})
				}

				router.Route("/logs", func(router chi.Router) {
					router.Get("/", listLogs(routerOptions.paginationConfig))
					router.Post("/import", importLogs)
					router.Post("/export", exportLogs)
				})

				router.Route("/accounts", func(router chi.Router) {
					router.Get("/", listAccounts(routerOptions.paginationConfig))
					router.Head("/", countAccounts)
					router.Get("/{address}", readAccount)
					router.Post("/{address}/metadata", addAccountMetadata)
					router.Delete("/{address}/metadata/{key}", deleteAccountMetadata)
				})

				router.Route("/transactions", func(router chi.Router) {
					router.Get("/", listTransactions(routerOptions.paginationConfig))
					router.Head("/", countTransactions)
					router.Post("/", createTransaction)
					router.Get("/{id}", readTransaction)
					router.Post("/{id}/revert", revertTransaction)
					router.Post("/{id}/metadata", addTransactionMetadata)
					router.Delete("/{id}/metadata/{key}", deleteTransactionMetadata)
				})

				router.Get("/aggregate/balances", readBalancesAggregated)

				router.Get("/volumes", readVolumes(routerOptions.paginationConfig))
			})
		})
	})

	return router
}

type routerOptions struct {
	tracer               trace.Tracer
	bulkerFactory        bulking.BulkerFactory
	bulkHandlerFactories map[string]bulking.HandlerFactory
	paginationConfig     common.PaginationConfig
	exporters            bool
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkHandlerFactories(bulkHandlerFactories map[string]bulking.HandlerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkHandlerFactories = bulkHandlerFactories
	}
}

func WithBulkerFactory(bulkerFactory bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bulkerFactory
	}
}

func WithPaginationConfig(paginationConfig common.PaginationConfig) RouterOption {
	return func(ro *routerOptions) {
		ro.paginationConfig = paginationConfig
	}
}

func WithExporters(v bool) RouterOption {
	return func(ro *routerOptions) {
		ro.exporters = v
	}
}

func WithDefaultBulkHandlerFactories(bulkMaxSize int) RouterOption {
	return WithBulkHandlerFactories(map[string]bulking.HandlerFactory{
		"application/json": bulking.NewJSONBulkHandlerFactory(bulkMaxSize),
		"application/vnd.formance.ledger.api.v2.bulk+script-stream": bulking.NewTextStreamBulkHandlerFactory(),
		"application/vnd.formance.ledger.api.v2.bulk+json-stream":   bulking.NewJSONStreamBulkHandlerFactory(),
	})
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkerFactory(bulking.NewDefaultBulkerFactory()),
	WithDefaultBulkHandlerFactories(100),
	WithPaginationConfig(common.PaginationConfig{
		DefaultPageSize: bunpaginate.QueryDefaultPageSize,
		MaxPageSize:     bunpaginate.MaxPageSize,
	}),
}