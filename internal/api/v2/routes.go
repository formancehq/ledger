package v2

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/stack/libs/go-libs/service"

	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/opentelemetry/metrics"
	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

func NewRouter(
	b backend.Backend,
	healthController *health.HealthController,
	globalMetricsRegistry metrics.GlobalRegistry,
	a auth.Auth,
) chi.Router {
	router := chi.NewMux()

	router.Use(
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
		}).Handler,
		MetricsMiddleware(globalMetricsRegistry),
		middleware.Recoverer,
	)

	router.Get("/_healthcheck", healthController.Check)
	router.Get("/_info", getInfo(b))

	router.Group(func(router chi.Router) {
		router.Use(auth.Middleware(a))
		router.Use(service.OTLPMiddleware("ledger"))

		router.Get("/", listLedgers(b))
		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					trace.
						SpanFromContext(r.Context()).
						SetAttributes(attribute.String("ledger", chi.URLParam(r, "ledger")))
					handler.ServeHTTP(w, r)
				})
			})
			router.Post("/", createLedger(b))
			router.Get("/", getLedger(b))
			router.Put("/metadata", updateLedgerMetadata(b))
			router.Delete("/metadata/{key}", deleteLedgerMetadata(b))

			router.With(backend.LedgerMiddleware(b, []string{"/_info"})).Group(func(router chi.Router) {
				router.Post("/_bulk", bulkHandler)

				// LedgerController
				router.Get("/_info", getLedgerInfo)
				router.Get("/stats", getStats)
				router.Get("/logs", getLogs)
				router.Post("/logs/import", importLogs)
				router.Post("/logs/export", exportLogs)

				// AccountController
				router.Get("/accounts", getAccounts)
				router.Head("/accounts", countAccounts)
				router.Get("/accounts/{address}", getAccount)
				router.Post("/accounts/{address}/metadata", postAccountMetadata)
				router.Delete("/accounts/{address}/metadata/{key}", deleteAccountMetadata)

				// TransactionController
				router.Get("/transactions", getTransactions)
				router.Head("/transactions", countTransactions)

				router.Post("/transactions", postTransaction)

				router.Get("/transactions/{id}", getTransaction)
				router.Post("/transactions/{id}/revert", revertTransaction)
				router.Post("/transactions/{id}/metadata", postTransactionMetadata)
				router.Delete("/transactions/{id}/metadata/{key}", deleteTransactionMetadata)

				router.Get("/aggregate/balances", getBalancesAggregated)

				router.Get("/volumes", getVolumesWithBalances)
			})
		})
	})

	return router
}
