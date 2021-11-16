package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/api/middlewares"
	"github.com/numary/ledger/ledger"
)

// Router -
func Router(cc cors.Config, resolver *ledger.Resolver) *gin.Engine {
	router := gin.Default()

	// Default Middlewares
	router.Use(
		cors.New(cc),
		gin.Recovery(),
		middlewares.AuthMiddleware(router),
	)

	// API Controllers
	configController := controllers.CreateConfigController()
	ledgerController := controllers.CreateLedgerController()
	transactionController := controllers.CreateTransactionController()
	accountController := controllers.CreateAccountController()
	scriptController := controllers.CreateScriptController()

	// API Routes
	router.GET("/_info", configController.GetInfo)

	ledgerGroup := router.Group("/:ledger", middlewares.LedgerMiddleware(resolver))
	{
		ledgerGroup.GET("/stats", ledgerController.GetStats)

		transactionGroup := ledgerGroup.Group("/transactions")
		{
			transactionGroup.GET("/", transactionController.GetTransactions)
			transactionGroup.POST("/", transactionController.PostTransaction)
			transactionGroup.POST("/:transactionId/revert", transactionController.RevertTransaction)
			transactionGroup.GET("/:transactionId/metadata", transactionController.GetTransactionMetadata)
		}

		accountGroup := ledgerGroup.Group("/accounts")
		{
			accountGroup.GET("/", accountController.GetAccounts)
			accountGroup.GET("/:accountId", accountController.GetAddress)
			accountGroup.GET("/:accountId/metadata", accountController.GetAccountMetadata)
		}

		scriptGroup := ledgerGroup.Group("/script")
		{
			scriptGroup.POST("/", scriptController.PostScript)
		}
	}

	return router
}
