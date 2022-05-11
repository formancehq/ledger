package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedauth"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
	"net/http"
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

// Routes -
type Routes struct {
	resolver              *ledger.Resolver
	ledgerMiddleware      middlewares.LedgerMiddleware
	healthController      controllers.HealthController
	configController      controllers.ConfigController
	ledgerController      controllers.LedgerController
	scriptController      controllers.ScriptController
	accountController     controllers.AccountController
	transactionController controllers.TransactionController
	mappingController     controllers.MappingController
	globalMiddlewares     []gin.HandlerFunc
	perLedgerMiddlewares  []gin.HandlerFunc
	useScopes             UseScopes
}

// NewRoutes -
func NewRoutes(
	globalMiddlewares []gin.HandlerFunc,
	perLedgerMiddlewares []gin.HandlerFunc,
	resolver *ledger.Resolver,
	ledgerMiddleware middlewares.LedgerMiddleware,
	configController controllers.ConfigController,
	ledgerController controllers.LedgerController,
	scriptController controllers.ScriptController,
	accountController controllers.AccountController,
	transactionController controllers.TransactionController,
	mappingController controllers.MappingController,
	healthController controllers.HealthController,
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
			ok = true
		})).ServeHTTP(context.Writer, context.Request)
		if !ok {
			context.AbortWithStatus(http.StatusForbidden)
		}
	}
}

// Engine -
func (r *Routes) Engine() *gin.Engine {
	engine := gin.New()

	// Default Middlewares
	engine.Use(r.globalMiddlewares...)

	engine.GET("/_health", r.healthController.Check)
	engine.GET("/swagger.yaml", r.configController.GetDocsAsYaml)
	engine.GET("/swagger.json", r.configController.GetDocsAsJSON)

	// API Routes
	engine.GET("/_info", r.configController.GetInfo)

	ledger := engine.Group("/:ledger", append(r.perLedgerMiddlewares, r.ledgerMiddleware.LedgerMiddleware())...)
	{
		// LedgerController
		ledger.GET("/stats", r.wrapWithScopes(r.ledgerController.GetStats, ScopesStatsRead))

		// TransactionController
		ledger.GET("/transactions", r.wrapWithScopes(r.transactionController.GetTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
		ledger.HEAD("/transactions", r.wrapWithScopes(r.transactionController.CountTransactions, ScopeTransactionsRead, ScopeTransactionsWrite))
		ledger.POST("/transactions", r.wrapWithScopes(r.transactionController.PostTransaction, ScopeTransactionsWrite))
		ledger.POST("/transactions/batch", r.wrapWithScopes(r.transactionController.PostTransactionsBatch, ScopeTransactionsWrite))
		ledger.GET("/transactions/:txid", r.wrapWithScopes(r.transactionController.GetTransaction, ScopeTransactionsRead, ScopeTransactionsWrite))
		ledger.POST("/transactions/:txid/revert", r.wrapWithScopes(r.transactionController.RevertTransaction, ScopeTransactionsWrite))
		ledger.POST("/transactions/:txid/metadata", r.wrapWithScopes(r.transactionController.PostTransactionMetadata, ScopeTransactionsWrite))

		// AccountController
		ledger.GET("/accounts", r.wrapWithScopes(r.accountController.GetAccounts, ScopeAccountsRead, ScopeAccountsWrite))
		ledger.HEAD("/accounts", r.wrapWithScopes(r.accountController.CountAccounts, ScopeAccountsRead, ScopeAccountsWrite))
		ledger.GET("/accounts/:address", r.wrapWithScopes(r.accountController.GetAccount, ScopeAccountsRead, ScopeAccountsWrite))
		ledger.POST("/accounts/:address/metadata", r.wrapWithScopes(r.accountController.PostAccountMetadata, ScopeAccountsWrite))

		// MappingController
		ledger.GET("/mapping", r.wrapWithScopes(r.mappingController.GetMapping, ScopeMappingRead, ScopeMappingWrite))
		ledger.PUT("/mapping", r.wrapWithScopes(r.mappingController.PutMapping, ScopeMappingWrite))

		// ScriptController
		ledger.POST("/script", r.wrapWithScopes(r.scriptController.PostScript, ScopeTransactionsWrite))
	}

	return engine
}
