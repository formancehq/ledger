package routes

import (
	"net/http"

	"github.com/formancehq/go-libs/sharedauth"
	sharedhealth "github.com/formancehq/go-libs/sharedhealth/pkg"
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

type UseScopes bool

const (
	ScopeTransactionsRead  = "transactions:read"
	ScopeTransactionsWrite = "transactions:write"
	ScopeAccountsRead      = "accounts:read"
	ScopeAccountsWrite     = "accounts:write"
	ScopeMappingRead       = "mapping:read"
	ScopeMappingWrite      = "mapping:write"
	ScopesStatsRead        = "stats"
)

var AllScopes = []string{
	ScopeTransactionsRead,
	ScopeAccountsWrite,
	ScopeTransactionsWrite,
	ScopeAccountsRead,
	ScopeMappingRead,
	ScopeMappingWrite,
	ScopesStatsRead,
}

type Routes struct {
	resolver              *ledger.Resolver
	ledgerMiddleware      middlewares.LedgerMiddleware
	healthController      *sharedhealth.HealthController
	configController      controllers.ConfigController
	ledgerController      controllers.LedgerController
	scriptController      controllers.ScriptController
	accountController     controllers.AccountController
	balanceController     controllers.BalanceController
	transactionController controllers.TransactionController
	mappingController     controllers.MappingController
	globalMiddlewares     []gin.HandlerFunc
	perLedgerMiddlewares  []gin.HandlerFunc
	useScopes             UseScopes
	idempotencyStore      storage.Driver[idempotency.Store]
	locker                middlewares.Locker
}

func NewRoutes(
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
	healthController *sharedhealth.HealthController,
	useScopes UseScopes,
	idempotencyStore storage.Driver[idempotency.Store],
	locker middlewares.Locker,
) *Routes {
	return &Routes{
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
		useScopes:             useScopes,
		idempotencyStore:      idempotencyStore,
		locker:                locker,
	}
}

func (r *Routes) wrapWithScopes(handler gin.HandlerFunc, scopes ...string) gin.HandlerFunc {
	if !r.useScopes {
		return handler
	}
	return func(context *gin.Context) {
		ok := false
		sharedauth.NeedOneOfScopes(scopes...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			context.Request = r
			ok = true
			handler(context)
		})).ServeHTTP(context.Writer, context.Request)
		if !ok {
			context.AbortWithStatus(http.StatusForbidden)
		}
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
	dedicatedLedgerRouter.Use(append(r.perLedgerMiddlewares, r.ledgerMiddleware.LedgerMiddleware())...)

	// LedgerController
	dedicatedLedgerRouter.GET("/stats", r.wrapWithScopes(r.ledgerController.GetStats, ScopesStatsRead))

	// AccountController
	dedicatedLedgerRouter.GET("/accounts", r.wrapWithScopes(r.accountController.GetAccounts, ScopeAccountsRead, ScopeAccountsWrite))
	dedicatedLedgerRouter.HEAD("/accounts", r.wrapWithScopes(r.accountController.CountAccounts, ScopeAccountsRead, ScopeAccountsWrite))
	dedicatedLedgerRouter.GET("/accounts/:address", r.wrapWithScopes(r.accountController.GetAccount, ScopeAccountsRead, ScopeAccountsWrite))
	dedicatedLedgerRouter.POST("/accounts/:address/metadata",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.accountController.PostAccountMetadata, ScopeAccountsWrite))

	// TransactionController
	dedicatedLedgerRouter.GET("/transactions", r.wrapWithScopes(r.transactionController.GetTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
	dedicatedLedgerRouter.HEAD("/transactions", r.wrapWithScopes(r.transactionController.CountTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
	dedicatedLedgerRouter.POST("/transactions",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.transactionController.PostTransaction, ScopeTransactionsWrite)).Use()
	dedicatedLedgerRouter.POST("/transactions/batch",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.transactionController.PostTransactionsBatch, ScopeTransactionsWrite))
	dedicatedLedgerRouter.GET("/transactions/:txid", r.wrapWithScopes(r.transactionController.GetTransaction, ScopeTransactionsRead, ScopeTransactionsWrite))
	dedicatedLedgerRouter.POST("/transactions/:txid/revert",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.transactionController.RevertTransaction, ScopeTransactionsWrite))
	dedicatedLedgerRouter.POST("/transactions/:txid/metadata",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.transactionController.PostTransactionMetadata, ScopeTransactionsWrite))

	// BalanceController
	dedicatedLedgerRouter.GET("/balances", r.wrapWithScopes(r.balanceController.GetBalances, ScopeAccountsRead))
	dedicatedLedgerRouter.GET("/aggregate/balances", r.wrapWithScopes(r.balanceController.GetBalancesAggregated, ScopeAccountsRead))

	// MappingController
	dedicatedLedgerRouter.GET("/mapping", r.wrapWithScopes(r.mappingController.GetMapping, ScopeMappingRead, ScopeMappingWrite))
	dedicatedLedgerRouter.PUT("/mapping", r.wrapWithScopes(r.mappingController.PutMapping, ScopeMappingWrite))

	// ScriptController
	dedicatedLedgerRouter.POST("/script",
		middlewares.Transaction(r.locker),
		idempotency.Middleware(r.idempotencyStore),
		r.wrapWithScopes(r.scriptController.PostScript, ScopeTransactionsWrite))

	return engine
}
