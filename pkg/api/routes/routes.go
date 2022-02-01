package routes

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/logging"
	"go.uber.org/fx"
	"time"
)

const GlobalMiddlewaresKey = `group:"_routesGlobalMiddlewares"`
const PerLedgerMiddlewaresKey = `group:"_perLedgerMiddlewares"`

var Module = fx.Options(
	fx.Provide(
		fx.Annotate(NewRoutes, fx.ParamTags(GlobalMiddlewaresKey, PerLedgerMiddlewaresKey)),
	),
)

func ProvideGlobalMiddleware(provider interface{}, additionalAnnotations ...fx.Annotation) fx.Option {
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
	authMiddleware        middlewares.AuthMiddleware
	ledgerMiddleware      middlewares.LedgerMiddleware
	configController      controllers.ConfigController
	ledgerController      controllers.LedgerController
	scriptController      controllers.ScriptController
	accountController     controllers.AccountController
	transactionController controllers.TransactionController
	mappingController     controllers.MappingController
	globalMiddlewares     []gin.HandlerFunc
	perLedgerMiddlewares  []gin.HandlerFunc
	logger                logging.Logger
}

// NewRoutes -
func NewRoutes(
	globalMiddlewares []gin.HandlerFunc,
	perLedgerMiddlewares []gin.HandlerFunc,
	resolver *ledger.Resolver,
	authMiddleware middlewares.AuthMiddleware,
	ledgerMiddleware middlewares.LedgerMiddleware,
	configController controllers.ConfigController,
	ledgerController controllers.LedgerController,
	scriptController controllers.ScriptController,
	accountController controllers.AccountController,
	transactionController controllers.TransactionController,
	mappingController controllers.MappingController,
	logger logging.Logger,
) *Routes {
	return &Routes{
		globalMiddlewares:     globalMiddlewares,
		perLedgerMiddlewares:  perLedgerMiddlewares,
		resolver:              resolver,
		authMiddleware:        authMiddleware,
		ledgerMiddleware:      ledgerMiddleware,
		configController:      configController,
		ledgerController:      ledgerController,
		scriptController:      scriptController,
		accountController:     accountController,
		transactionController: transactionController,
		mappingController:     mappingController,
		logger:                logger,
	}
}

// Engine -
func (r *Routes) Engine(cc cors.Config) *gin.Engine {
	engine := gin.New()

	globalMiddlewares := append([]gin.HandlerFunc{
		cors.New(cc),
		gin.Recovery(),
		func(c *gin.Context) {
			start := time.Now()
			c.Next()
			latency := time.Now().Sub(start)
			r.logger.WithFields(map[string]interface{}{
				"status":     c.Writer.Status(),
				"method":     c.Request.Method,
				"path":       c.Request.URL.Path,
				"ip":         c.ClientIP(),
				"latency":    latency,
				"user_agent": c.Request.UserAgent(),
			}).Info(c.Request.Context(), "Request")
		},
	}, r.globalMiddlewares...)

	// Default Middlewares
	engine.Use(globalMiddlewares...)

	engine.GET("/swagger.yaml", r.configController.GetDocsAsYaml)
	//engine.GET("/swagger.json", r.configController.GetDocsAsJSON)

	// API Routes
	engine.GET("/_info", r.configController.GetInfo)

	ledger := engine.Group("/:ledger", append(r.perLedgerMiddlewares, r.ledgerMiddleware.LedgerMiddleware())...)
	{
		// LedgerController
		ledger.GET("/stats", r.ledgerController.GetStats)

		// TransactionController
		ledger.GET("/transactions", r.transactionController.GetTransactions)
		ledger.POST("/transactions", r.transactionController.PostTransaction)
		ledger.POST("/transactions/batch", r.transactionController.PostTransactionsBatch)
		ledger.GET("/transactions/:txid", r.transactionController.GetTransaction)
		ledger.POST("/transactions/:txid/revert", r.transactionController.RevertTransaction)
		ledger.POST("/transactions/:txid/metadata", r.transactionController.PostTransactionMetadata)

		// AccountController
		ledger.GET("/accounts", r.accountController.GetAccounts)
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
