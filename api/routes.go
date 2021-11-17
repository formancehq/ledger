package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/api/middlewares"
	"github.com/numary/ledger/ledger"
)

// NewRoutes -
func NewRoutes(
	cc cors.Config,
	resolver *ledger.Resolver,
	configController *controllers.ConfigController,
	ledgerController *controllers.LedgerController,
	scriptController *controllers.ScriptController,
	accountController *controllers.AccountController,
	transactionController *controllers.TransactionController,
) *gin.Engine {
	engine := gin.Default()

	// Default Middlewares
	engine.Use(
		cors.New(cc),
		gin.Recovery(),
		middlewares.AuthMiddleware(engine),
	)

	// API Routes
	engine.GET("/_info", configController.GetInfo)

	ledgerGroup := engine.Group("/:ledger", middlewares.LedgerMiddleware(resolver))
	{
		// LedgerController
		ledgerGroup.GET("/stats", ledgerController.GetStats)

		// TransactionController
		ledgerGroup.GET("/transactions", transactionController.GetTransactions)
		ledgerGroup.POST("/transactions", transactionController.PostTransaction)
		ledgerGroup.POST("/transactions/:transactionId/revert", transactionController.RevertTransaction)
		ledgerGroup.GET("/transactions/:transactionId/metadata", transactionController.GetTransactionMetadata)

		// AccountController
		ledgerGroup.GET("/accounts", accountController.GetAccounts)
		ledgerGroup.GET("/accounts/:accountId", accountController.GetAddress)
		ledgerGroup.GET("/accounts/:accountId/metadata", accountController.GetAccountMetadata)

		// ScriptController
		ledgerGroup.POST("/script", scriptController.PostScript)
	}

	return engine
}
