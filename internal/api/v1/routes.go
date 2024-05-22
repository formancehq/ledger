package v1

import (
	"net/http"

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

		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handler.ServeHTTP(w, r)
				})
			})
			router.Use(autoCreateMiddleware(b))
			router.Use(backend.LedgerMiddleware(b, []string{"/_info"}))

			// LedgerController
			router.Get("/_info", getLedgerInfo)
			router.Get("/stats", getStats)
			router.Get("/logs", getLogs)

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
			router.Post("/transactions/batch", func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "not supported", http.StatusBadRequest)
			})

			router.Get("/transactions/{id}", getTransaction)
			router.Post("/transactions/{id}/revert", revertTransaction)
			router.Post("/transactions/{id}/metadata", postTransactionMetadata)
			router.Delete("/transactions/{id}/metadata/{key}", deleteTransactionMetadata)

			router.Get("/balances", getBalances)
			router.Get("/aggregate/balances", getBalancesAggregated)
		})
	})

	return router
}
