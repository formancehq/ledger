package routes

import (
	"github.com/gin-gonic/gin"
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
		ledger.GET("/stats", r.ledgerController.GetStats)

		// TransactionController
		ledger.GET("/transactions", r.transactionController.GetTransactions)
		ledger.HEAD("/transactions", r.transactionController.CountTransactions)
		ledger.POST("/transactions", r.transactionController.PostTransaction)
		ledger.POST("/transactions/batch", r.transactionController.PostTransactionsBatch)
		ledger.GET("/transactions/:txid", r.transactionController.GetTransaction)
		ledger.POST("/transactions/:txid/revert", r.transactionController.RevertTransaction)
		ledger.POST("/transactions/:txid/metadata", r.transactionController.PostTransactionMetadata)

		// AccountController
		ledger.GET("/accounts", r.accountController.GetAccounts)
		ledger.HEAD("/accounts", r.accountController.CountAccounts)
		ledger.GET("/accounts/:address", r.accountController.GetAccount)
		ledger.POST("/accounts/:address/metadata", r.accountController.PostAccountMetadata)

		// MappingController
		ledger.GET("/mapping", r.mappingController.GetMapping)
		ledger.PUT("/mapping", r.mappingController.PutMapping)

		// ScriptController
		ledger.POST("/script", r.scriptController.PostScript)
	}

	return engine
}
