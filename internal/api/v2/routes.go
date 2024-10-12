package v2

import (
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/api"
	"github.com/go-chi/chi/v5/middleware"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/service"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func NewRouter(
	systemController system.Controller,
	authenticator auth.Authenticator,
	version string,
	debug bool,
	opts ...RouterOption,
) chi.Router {
	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	router := chi.NewMux()

	router.Get("/_info", getInfo(version))

	router.Group(func(router chi.Router) {
		router.Use(middleware.RequestLogger(api.NewLogFormatter()))
		router.Use(auth.Middleware(authenticator))
		router.Use(service.OTLPMiddleware("ledger", debug))

		router.Get("/", listLedgers(systemController))
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
				router.Post("/_bulk", bulkHandler)

				// LedgerController
				router.Get("/_info", getLedgerInfo)
				router.Get("/stats", readStats)
				router.Get("/logs", listLogs)
				router.Post("/logs/import", importLogs)
				router.Post("/logs/export", exportLogs)

				// AccountController
				router.Get("/accounts", listAccounts)
				router.Head("/accounts", countAccounts)
				router.Get("/accounts/{address}", readAccount)
				router.Post("/accounts/{address}/metadata", addAccountMetadata)
				router.Delete("/accounts/{address}/metadata/{key}", deleteAccountMetadata)

				// TransactionController
				router.Get("/transactions", listTransactions)
				router.Head("/transactions", countTransactions)

				router.Post("/transactions", createTransaction)

				router.Get("/transactions/{id}", readTransaction)
				router.Post("/transactions/{id}/revert", revertTransaction)
				router.Post("/transactions/{id}/metadata", addTransactionMetadata)
				router.Delete("/transactions/{id}/metadata/{key}", deleteTransactionMetadata)

				router.Get("/aggregate/balances", readBalancesAggregated)

				router.Get("/volumes", readVolumes)
			})
		})
	})

	return router
}

type routerOptions struct {
	tracer trace.Tracer
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
}