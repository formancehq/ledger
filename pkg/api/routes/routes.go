package routes

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/middlewares"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/riandyrn/otelchi"
)

func NewRouter(storageDriver storage.Driver, version string, resolver *ledger.Resolver,
	logger logging.Logger, healthController *health.HealthController) chi.Router {
	router := chi.NewMux()

	router.Use(
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
		}).Handler,
		func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(
					logging.ContextWithLogger(r.Context(), logger),
				))
			})
		},
		middlewares.Log(),
		middleware.Recoverer,
	)
	router.Use()
	router.Use(middlewares.Log())

	router.Get("/_healthcheck", healthController.Check)

	router.Group(func(router chi.Router) {
		router.Use(otelchi.Middleware("ledger"))
		router.Get("/_info", controllers.GetInfo(storageDriver, version))

		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handler.ServeHTTP(w, r)
				})
			})
			router.Use(middlewares.LedgerMiddleware(resolver))

			// LedgerController
			router.Get("/_info", controllers.GetLedgerInfo)
			router.Get("/stats", controllers.GetStats)
			router.Get("/logs", controllers.GetLogs)

			// AccountController
			router.Get("/accounts", controllers.GetAccounts)
			router.Head("/accounts", controllers.CountAccounts)
			router.Get("/accounts/{address}", controllers.GetAccount)
			router.Post("/accounts/{address}/metadata", controllers.PostAccountMetadata)

			// TransactionController
			router.Get("/transactions", controllers.GetTransactions)
			router.Head("/transactions", controllers.CountTransactions)

			router.Post("/transactions", controllers.PostTransaction)

			router.Get("/transactions/{txid}", controllers.GetTransaction)
			router.Post("/transactions/{txid}/revert", controllers.RevertTransaction)
			router.Post("/transactions/{txid}/metadata", controllers.PostTransactionMetadata)

			// BalanceController
			router.Get("/balances", controllers.GetBalances)
			// TODO: Rename to /aggregatedBalances
			router.Get("/aggregate/balances", controllers.GetBalancesAggregated)
		})
	})

	return router
}
