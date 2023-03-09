package routes

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/idempotency"
	"github.com/formancehq/ledger/pkg/api/middlewares"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	"go.uber.org/fx"
)

const GlobalMiddlewaresKey = `name:"_routesGlobalMiddlewares" optional:"true"`

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewRoutes, fx.ParamTags(GlobalMiddlewaresKey)),
	),
)

func ProvideMiddlewares(provider interface{}, additionalAnnotations ...fx.Annotation) fx.Option {
	opts := []fx.Annotation{fx.ResultTags(GlobalMiddlewaresKey)}
	return fx.Provide(
		fx.Annotate(provider, append(opts, additionalAnnotations...)...),
	)
}

type Routes struct {
	resolver              *ledger.Resolver
	ledgerMiddleware      middlewares.LedgerMiddleware
	healthController      *health.HealthController
	configController      controllers.ConfigController
	ledgerController      controllers.LedgerController
	accountController     controllers.AccountController
	balanceController     controllers.BalanceController
	transactionController controllers.TransactionController
	globalMiddlewares     []func(handler http.Handler) http.Handler
	idempotencyStore      storage.Driver[idempotency.Store]
	locker                ledger.Locker
}

func NewRoutes(
	globalMiddlewares []func(handler http.Handler) http.Handler,
	resolver *ledger.Resolver,
	ledgerMiddleware middlewares.LedgerMiddleware,
	configController controllers.ConfigController,
	ledgerController controllers.LedgerController,
	accountController controllers.AccountController,
	balanceController controllers.BalanceController,
	transactionController controllers.TransactionController,
	healthController *health.HealthController,
	idempotencyStore storage.Driver[idempotency.Store],
	locker ledger.Locker,
) *Routes {
	return &Routes{
		globalMiddlewares:     globalMiddlewares,
		resolver:              resolver,
		ledgerMiddleware:      ledgerMiddleware,
		configController:      configController,
		ledgerController:      ledgerController,
		accountController:     accountController,
		balanceController:     balanceController,
		transactionController: transactionController,
		healthController:      healthController,
		idempotencyStore:      idempotencyStore,
		locker:                locker,
	}
}

func (r *Routes) Engine() *chi.Mux {
	router := chi.NewMux()

	router.Use(r.globalMiddlewares...)

	// Deprecated
	router.Get("/_health", r.healthController.Check)
	router.Get("/_healthcheck", r.healthController.Check)

	router.Group(func(router chi.Router) {
		router.Use(otelchi.Middleware("ledger"))
		router.Get("/_info", r.configController.GetInfo)

		router.Route("/{ledger}", func(router chi.Router) {
			router.Use(func(handler http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handler.ServeHTTP(w, r)
				})
			})
			router.Use(r.ledgerMiddleware.LedgerMiddleware())

			// LedgerController
			router.Get("/_info", r.ledgerController.GetInfo)
			router.Get("/stats", r.ledgerController.GetStats)
			router.Get("/logs", r.ledgerController.GetLogs)

			// AccountController
			router.Get("/accounts", r.accountController.GetAccounts)
			router.Head("/accounts", r.accountController.CountAccounts)
			router.Get("/accounts/{address}", r.accountController.GetAccount)
			router.With(
				middlewares.Lock(r.locker),
				idempotency.Middleware(r.idempotencyStore),
			).Post("/accounts/{address}/metadata", r.accountController.PostAccountMetadata)

			// TransactionController
			router.Get("/transactions", r.transactionController.GetTransactions)
			router.Head("/transactions", r.transactionController.CountTransactions)

			router.With(
				middlewares.Lock(r.locker),
				idempotency.Middleware(r.idempotencyStore),
			).Post("/transactions", r.transactionController.PostTransaction)

			router.Get("/transactions/{txid}", r.transactionController.GetTransaction)
			router.With(
				middlewares.Lock(r.locker),
				idempotency.Middleware(r.idempotencyStore),
			).Post("/transactions/{txid}/revert", r.transactionController.RevertTransaction)
			router.With(
				middlewares.Lock(r.locker),
				idempotency.Middleware(r.idempotencyStore),
			).Post("/transactions/{txid}/metadata", r.transactionController.PostTransactionMetadata)

			// BalanceController
			router.Get("/balances", r.balanceController.GetBalances)
			// TODO: Rename to /aggregatedBalances
			router.Get("/aggregate/balances", r.balanceController.GetBalancesAggregated)
		})
	})

	return router
}
