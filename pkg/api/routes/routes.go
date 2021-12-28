package routes

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/logger"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/ledger"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewRoutes),
)

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
	tracerProvider        trace.TracerProvider
}

// NewRoutes -
func NewRoutes(
	tracerProvider trace.TracerProvider,
	resolver *ledger.Resolver,
	authMiddleware middlewares.AuthMiddleware,
	ledgerMiddleware middlewares.LedgerMiddleware,
	configController controllers.ConfigController,
	ledgerController controllers.LedgerController,
	scriptController controllers.ScriptController,
	accountController controllers.AccountController,
	transactionController controllers.TransactionController,
) *Routes {
	return &Routes{
		tracerProvider:        tracerProvider,
		resolver:              resolver,
		authMiddleware:        authMiddleware,
		ledgerMiddleware:      ledgerMiddleware,
		configController:      configController,
		ledgerController:      ledgerController,
		scriptController:      scriptController,
		accountController:     accountController,
		transactionController: transactionController,
	}
}

// Engine -
func (r *Routes) Engine(cc cors.Config) *gin.Engine {
	engine := gin.New()

	// Default Middlewares
	engine.Use(
		cors.New(cc),
		gin.Recovery(),
		logger.SetLogger(),
		otelgin.Middleware("ledger", otelgin.WithTracerProvider(r.tracerProvider)),
	)

	engine.GET("/swagger.json", r.configController.GetDocs)

	// API Routes
	engine.GET("/_info", r.configController.GetInfo)

	ledger := engine.Group("/:ledger", r.ledgerMiddleware.LedgerMiddleware())
	{
		// LedgerController
		ledger.GET("/stats", r.ledgerController.GetStats)

		// TransactionController
		ledger.GET("/transactions", r.transactionController.GetTransactions)
		ledger.POST("/transactions", r.transactionController.PostTransaction)
		ledger.GET("/transactions/:txid", r.transactionController.GetTransaction)
		ledger.POST("/transactions/:txid/revert", r.transactionController.RevertTransaction)
		ledger.POST("/transactions/:txid/metadata", r.transactionController.PostTransactionMetadata)

		// AccountController
		ledger.GET("/accounts", r.accountController.GetAccounts)
		ledger.GET("/accounts/:address", r.accountController.GetAccount)
		ledger.POST("/accounts/:address/metadata", r.accountController.PostAccountMetadata)

		// ScriptController
		ledger.POST("/script", r.scriptController.PostScript)
	}

	return engine
}
