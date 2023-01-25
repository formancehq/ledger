package api

import (
	"net/http"

	sharedapi "github.com/formancehq/go-libs/api"
	sharedhealth "github.com/formancehq/go-libs/health"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/riandyrn/otelchi"
)

func NewRouter(
	manager *wallet.Manager,
	healthController *sharedhealth.HealthController,
	serviceInfo sharedapi.ServiceInfo,
) *chi.Mux {
	r := chi.NewRouter()

	r.Get("/_healthcheck", healthController.Check)
	r.Get("/_info", sharedapi.InfoHandler(serviceInfo))
	r.Group(func(r chi.Router) {
		r.Use(otelchi.Middleware("wallets"))
		r.Use(middleware.Logger)
		r.Use(middleware.AllowContentType("application/json"))
		r.Use(func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				handler.ServeHTTP(w, r)
			})
		})
		main := NewMainHandler(manager)

		r.Route("/wallets", func(r chi.Router) {
			r.Get("/", main.listWalletsHandler)
			r.Post("/", main.createWalletHandler)
			r.Route("/{walletID}", func(r chi.Router) {
				r.Get("/", main.getWalletHandler)
				r.Patch("/", main.patchWalletHandler)
				r.Post("/debit", main.debitWalletHandler)
				r.Post("/credit", main.creditWalletHandler)
				r.Route("/balances", func(r chi.Router) {
					r.Get("/", main.listBalancesHandler)
					r.Post("/", main.createBalanceHandler)
					r.Get("/{balanceName}", main.getBalanceHandler)
				})
			})
		})
		r.Route("/transactions", func(r chi.Router) {
			r.Get("/", main.listTransactions)
		})
		r.Route("/holds", func(r chi.Router) {
			r.Get("/", main.listHoldsHandler)
			r.Route("/{holdID}", func(r chi.Router) {
				r.Get("/", main.getHoldHandler)
				r.Post("/confirm", main.confirmHoldHandler)
				r.Post("/void", main.voidHoldHandler)
			})
		})
	})

	return r
}
