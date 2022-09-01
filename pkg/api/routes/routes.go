package routes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedauth"
	sharedhealth "github.com/numary/go-libs/sharedhealth/pkg"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/ledger"
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

	engine.GET("/_health", func(context *gin.Context) {
		r.healthController.Check(context.Writer, context.Request)
	})
	engine.GET("/swagger.yaml", r.configController.GetDocsAsYaml)
	engine.GET("/swagger.json", r.configController.GetDocsAsJSON)

	engine.GET("/_info", r.configController.GetInfo)

	router := engine.Group("/:ledger", append(r.perLedgerMiddlewares, r.ledgerMiddleware.LedgerMiddleware())...)
	{
		// LedgerController
		router.GET("/stats", r.wrapWithScopes(r.ledgerController.GetStats, ScopesStatsRead))

		// AccountController
		router.GET("/accounts", r.wrapWithScopes(r.accountController.GetAccounts, ScopeAccountsRead, ScopeAccountsWrite))
		router.HEAD("/accounts", r.wrapWithScopes(r.accountController.CountAccounts, ScopeAccountsRead, ScopeAccountsWrite))
		router.GET("/accounts/:address", r.wrapWithScopes(r.accountController.GetAccount, ScopeAccountsRead, ScopeAccountsWrite))
		router.POST("/accounts/:address/metadata", r.wrapWithScopes(r.accountController.PostAccountMetadata, ScopeAccountsWrite))

		// TransactionController
		router.GET("/transactions", r.wrapWithScopes(r.transactionController.GetTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
		router.HEAD("/transactions", r.wrapWithScopes(r.transactionController.CountTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
		router.POST("/transactions", r.wrapWithScopes(r.transactionController.PostTransaction, ScopeTransactionsWrite))
		router.POST("/transactions/batch", r.wrapWithScopes(r.transactionController.PostTransactionsBatch, ScopeTransactionsWrite))
		router.GET("/transactions/:txid", r.wrapWithScopes(r.transactionController.GetTransaction, ScopeTransactionsRead, ScopeTransactionsWrite))
		router.POST("/transactions/:txid/revert", r.wrapWithScopes(r.transactionController.RevertTransaction, ScopeTransactionsWrite))
		router.POST("/transactions/:txid/metadata", r.wrapWithScopes(r.transactionController.PostTransactionMetadata, ScopeTransactionsWrite))

		// BalanceController
		router.GET("/balances", r.wrapWithScopes(r.balanceController.GetBalances, ScopeAccountsRead))
		router.GET("/aggregate/balances", r.wrapWithScopes(r.balanceController.GetBalancesAggregated, ScopeAccountsRead))

		// MappingController
		router.GET("/mapping", r.wrapWithScopes(r.mappingController.GetMapping, ScopeMappingRead, ScopeMappingWrite))
		router.PUT("/mapping", r.wrapWithScopes(r.mappingController.PutMapping, ScopeMappingWrite))

		// ScriptController
		router.POST("/script", r.wrapWithScopes(r.scriptController.PostScript, ScopeTransactionsWrite))
	}

	return engine
}
