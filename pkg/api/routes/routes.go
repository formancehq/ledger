package routes

import (
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/idempotency"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/fx"
)

const GlobalMiddlewaresKey = `name:"_routesGlobalMiddlewares" optional:"true"`
const PerLedgerMiddlewaresKey = `name:"_perLedgerMiddlewares" optional:"true"`

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewRoutes, fx.ParamTags(GlobalMiddlewaresKey, PerLedgerMiddlewaresKey)),
	),
)

func ProvideMiddlewares(provider interface{}, additionalAnnotations ...fx.Annotation) fx.Option {
	opts := []fx.Annotation{fx.ResultTags(GlobalMiddlewaresKey)}
	return fx.Provide(
		fx.Annotate(provider, append(opts, additionalAnnotations...)...),
	)
}

func ProvidePerLedgerMiddleware(provider interface{}, additionalAnnotations ...fx.Annotation) fx.Option {
	opts := []fx.Annotation{fx.ResultTags(PerLedgerMiddlewaresKey)}
	return fx.Provide(
		fx.Annotate(provider, append(opts, additionalAnnotations...)...),
	)
}

type Routes struct {
	a                     auth.Auth
	resolver              *ledger.Resolver
	ledgerMiddleware      middlewares.LedgerMiddleware
	healthController      *health.HealthController
	configController      controllers.ConfigController
	ledgerController      controllers.LedgerController
	scriptController      controllers.ScriptController
	accountController     controllers.AccountController
	balanceController     controllers.BalanceController
	transactionController controllers.TransactionController
	mappingController     controllers.MappingController
	globalMiddlewares     []gin.HandlerFunc
	perLedgerMiddlewares  []gin.HandlerFunc
	idempotencyStore      storage.Driver[idempotency.Store]
	locker                middlewares.Locker
}

func NewRoutes(
	a auth.Auth,
	globalMiddlewares []gin.HandlerFunc,
	perLedgerMiddlewares []gin.HandlerFunc,
	resolver *ledger.Resolver,
	ledgerMiddleware middlewares.LedgerMiddleware,
	configController controllers.ConfigController,
	ledgerController controllers.LedgerController,
	scriptController controllers.ScriptController,
	accountController controllers.AccountController,
	balanceController controllers.BalanceController,
	transactionController controllers.TransactionController,
	mappingController controllers.MappingController,
	healthController *health.HealthController,
	idempotencyStore storage.Driver[idempotency.Store],
	locker middlewares.Locker,
) *Routes {
	return &Routes{
		a:                     a,
		globalMiddlewares:     globalMiddlewares,
		perLedgerMiddlewares:  perLedgerMiddlewares,
		resolver:              resolver,
		ledgerMiddleware:      ledgerMiddleware,
		configController:      configController,
		ledgerController:      ledgerController,
		scriptController:      scriptController,
		accountController:     accountController,
		balanceController:     balanceController,
		transactionController: transactionController,
		mappingController:     mappingController,
		healthController:      healthController,
		idempotencyStore:      idempotencyStore,
		locker:                locker,
	}
}

func (r *Routes) Engine() *gin.Engine {
	engine := gin.New()

	engine.Use(r.globalMiddlewares...)

	// Deprecated
	engine.GET("/_health", func(context *gin.Context) {
		r.healthController.Check(context.Writer, context.Request)
	})
	engine.GET("/_healthcheck", func(context *gin.Context) {
		r.healthController.Check(context.Writer, context.Request)
	})
	engine.GET("/swagger.yaml", r.configController.GetDocsAsYaml)
	engine.GET("/swagger.json", r.configController.GetDocsAsJSON)

	engineWithOtel := engine.Group("/")
	engineWithOtel.Use(otelgin.Middleware("ledger"))
	engineWithOtel.GET("/_info", r.configController.GetInfo)

	dedicatedLedgerRouter := engineWithOtel.Group("/:ledger")
	dedicatedLedgerRouter.Use(func(c *gin.Context) {
		handled := false
		auth.Middleware(r.a)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handled = true
			c.Next()
		})).ServeHTTP(c.Writer, c.Request)
		if !handled {
			c.Abort()
		}
	})
	dedicatedLedgerRouter.Use(append(r.perLedgerMiddlewares, r.ledgerMiddleware.LedgerMiddleware())...)

	// LedgerController
	dedicatedLedgerRouter.GET("/_info", r.ledgerController.GetInfo)
	dedicatedLedgerRouter.GET("/stats", r.ledgerController.GetStats)
	dedicatedLedgerRouter.GET("/logs", r.ledgerController.GetLogs)

	// AccountController
	dedicatedLedgerRouter.GET("/accounts", r.accountController.GetAccounts)
	dedicatedLedgerRouter.HEAD("/accounts", r.accountController.CountAccounts)
	dedicatedLedgerRouter.GET("/accounts/:address", r.accountController.GetAccount)
	dedicatedLedgerRouter.POST("/accounts/:address/metadata",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.accountController.PostAccountMetadata)

	// TransactionController
	dedicatedLedgerRouter.GET("/transactions", r.transactionController.GetTransactions)
	dedicatedLedgerRouter.HEAD("/transactions", r.transactionController.CountTransactions)
	dedicatedLedgerRouter.POST("/transactions",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.transactionController.PostTransaction).Use()
	dedicatedLedgerRouter.POST("/transactions/batch",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.transactionController.PostTransactionsBatch)
	dedicatedLedgerRouter.GET("/transactions/:txid", r.transactionController.GetTransaction)
	dedicatedLedgerRouter.POST("/transactions/:txid/revert",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.transactionController.RevertTransaction)
	dedicatedLedgerRouter.POST("/transactions/:txid/metadata",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.transactionController.PostTransactionMetadata)

	// BalanceController
	dedicatedLedgerRouter.GET("/balances", r.balanceController.GetBalances)
	dedicatedLedgerRouter.GET("/aggregate/balances", r.balanceController.GetBalancesAggregated)

	// MappingController
	dedicatedLedgerRouter.GET("/mapping", r.mappingController.GetMapping)
	dedicatedLedgerRouter.PUT("/mapping", r.mappingController.PutMapping)

	// ScriptController
	dedicatedLedgerRouter.POST("/script",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.scriptController.PostScript)

	return engine
}
