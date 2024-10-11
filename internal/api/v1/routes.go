package v1

import (
	"net/http"

	"github.com/formancehq/ledger/internal/controller/system"

	"github.com/formancehq/go-libs/api"
	"github.com/go-chi/chi/v5/middleware"

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
) chi.Router {
	router := chi.NewMux()

	router.Get("/_info", getInfo(systemController, version))

	router.Group(func(router chi.Router) {
		router.Use(middleware.RequestLogger(api.NewLogFormatter()))
		router.Use(auth.Middleware(authenticator))
		router.Use(service.OTLPMiddleware("ledger", debug))

		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handler.ServeHTTP(w, r)
				})
			})
			router.Use(autoCreateMiddleware(systemController))
			router.Use(common.LedgerMiddleware(systemController, func(r *http.Request) string {
				return chi.URLParam(r, "ledger")
			}, "/_info"))

			// LedgerController
			router.Get("/_info", getLedgerInfo)
			router.Get("/stats", getStats)
			router.Get("/logs", getLogs)

			// AccountController
			router.Get("/accounts", listAccounts)
			router.Head("/accounts", countAccounts)
			router.Get("/accounts/{address}", getAccount)
			router.Post("/accounts/{address}/metadata", addAccountMetadata)
			router.Delete("/accounts/{address}/metadata/{key}", deleteAccountMetadata)

			// TransactionController
			router.Get("/transactions", listTransactions)
			router.Head("/transactions", countTransactions)

			router.Post("/transactions", createTransaction)
			router.Post("/transactions/batch", func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "not supported", http.StatusBadRequest)
			})

			router.Get("/transactions/{id}", readTransaction)
			router.Post("/transactions/{id}/revert", revertTransaction)
			router.Post("/transactions/{id}/metadata", addTransactionMetadata)
			router.Delete("/transactions/{id}/metadata/{key}", deleteTransactionMetadata)

			router.Get("/balances", getBalances)
			router.Get("/aggregate/balances", getBalancesAggregated)
		})
	})

	return router
}
