package v2

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/bulking"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v2/service"

	"github.com/formancehq/go-libs/v2/auth"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func NewRouter(
	systemController system.Controller,
	authenticator auth.Authenticator,
	debug bool,
	opts ...RouterOption,
) chi.Router {
	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	router := chi.NewMux()

	router.Group(func(router chi.Router) {
		router.Use(routerOptions.middlewares...)
		router.Use(auth.Middleware(authenticator))
		router.Use(service.OTLPMiddleware("ledger", debug))

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

				// LedgerController
				router.Get("/_info", getLedgerInfo)
				router.Get("/stats", readStats)
				router.Get("/logs", listLogs(routerOptions.paginationConfig))
				router.Post("/logs/import", importLogs)
				router.Post("/logs/export", exportLogs)

				// AccountController
				router.Get("/accounts", listAccounts(routerOptions.paginationConfig))
				router.Head("/accounts", countAccounts)
				router.Get("/accounts/{address}", readAccount)
				router.Post("/accounts/{address}/metadata", addAccountMetadata)
				router.Delete("/accounts/{address}/metadata/{key}", deleteAccountMetadata)

				// TransactionController
				router.Get("/transactions", listTransactions(routerOptions.paginationConfig))
				router.Head("/transactions", countTransactions)

				router.Post("/transactions", createTransaction)

				router.Get("/transactions/{id}", readTransaction)
				router.Post("/transactions/{id}/revert", revertTransaction)
				router.Post("/transactions/{id}/metadata", addTransactionMetadata)
				router.Delete("/transactions/{id}/metadata/{key}", deleteTransactionMetadata)

				router.Get("/aggregate/balances", readBalancesAggregated)

				router.Get("/volumes", readVolumes(routerOptions.paginationConfig))
			})
		})
	})

	return router
}

type routerOptions struct {
	tracer               trace.Tracer
	middlewares          []func(http.Handler) http.Handler
	bulkerFactory        bulking.BulkerFactory
	bulkHandlerFactories map[string]bulking.HandlerFactory
	paginationConfig     common.PaginationConfig
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithMiddlewares(middlewares ...func(http.Handler) http.Handler) RouterOption {
	return func(ro *routerOptions) {
		ro.middlewares = append(ro.middlewares, middlewares...)
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

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkerFactory(bulking.NewDefaultBulkerFactory()),
	WithBulkHandlerFactories(map[string]bulking.HandlerFactory{
		"application/json": bulking.NewJSONBulkHandlerFactory(100),
		"application/vnd.formance.ledger.api.v2.bulk+script-stream": bulking.NewScriptStreamBulkHandlerFactory(),
	}),
	WithPaginationConfig(common.PaginationConfig{
		DefaultPageSize: bunpaginate.QueryDefaultPageSize,
		MaxPageSize: bunpaginate.MaxPageSize,
	}),
}
