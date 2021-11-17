package routes

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/api/middlewares"
	"github.com/numary/ledger/ledger"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewRoutes),
)

// Routes -
type Routes struct {
	resolver              *ledger.Resolver
	configController      *controllers.ConfigController
	ledgerController      *controllers.LedgerController
	scriptController      *controllers.ScriptController
	accountController     *controllers.AccountController
	transactionController *controllers.TransactionController
}

// NewRoutes -
func NewRoutes(
	resolver *ledger.Resolver,
	configController *controllers.ConfigController,
	ledgerController *controllers.LedgerController,
	scriptController *controllers.ScriptController,
	accountController *controllers.AccountController,
	transactionController *controllers.TransactionController,
) *Routes {
	return &Routes{
		resolver:              resolver,
		configController:      configController,
		ledgerController:      ledgerController,
		scriptController:      scriptController,
		accountController:     accountController,
		transactionController: transactionController,
	}
}

// Engine -
func (r *Routes) Engine(cc cors.Config) *gin.Engine {
	engine := gin.Default()

	// Default Middlewares
	engine.Use(
		cors.New(cc),
		gin.Recovery(),
		middlewares.AuthMiddleware(engine),
	)

	// API Routes
	engine.GET("/_info", r.configController.GetInfo)

	ledgerGroup := engine.Group("/:ledger", middlewares.LedgerMiddleware(r.resolver))
	{
		// LedgerController
		ledgerGroup.GET("/stats", r.ledgerController.GetStats)

		// TransactionController
		ledgerGroup.GET("/transactions", r.transactionController.GetTransactions)
		ledgerGroup.POST("/transactions", r.transactionController.PostTransaction)
		ledgerGroup.POST("/transactions/:transactionId/revert", r.transactionController.RevertTransaction)
		ledgerGroup.GET("/transactions/:transactionId/metadata", r.transactionController.GetTransactionMetadata)

		// AccountController
		ledgerGroup.GET("/accounts", r.accountController.GetAccounts)
		ledgerGroup.GET("/accounts/:accountId", r.accountController.GetAddress)
		ledgerGroup.GET("/accounts/:accountId/metadata", r.accountController.GetAccountMetadata)

		// ScriptController
		ledgerGroup.POST("/script", r.scriptController.PostScript)
	}

	return engine
}
