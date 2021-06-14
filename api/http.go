package api

import (
	"context"
	_ "embed"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
	"numary.io/ledger/ledger"
)

type HttpAPI struct {
	engine *gin.Engine
}

func NewHttpAPI(lc fx.Lifecycle, l *ledger.Ledger) *HttpAPI {
	r := gin.Default()

	r.GET("/_info", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"server":  "numary-ledger",
			"version": "1.0.0-alpha.1",
		})
	})

	r.GET("/ledger/:name", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"ok":    true,
			"stats": l.Stats(),
		})
	})

	r.GET("/transactions", func(c *gin.Context) {
		results, err := l.FindTransactions()

		c.JSON(200, gin.H{
			"ok":           err == nil,
			"transactions": results,
		})
	})

	r.POST("/transactions", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"ok": true,
		})
	})

	r.GET("/postings", func(c *gin.Context) {
		results, err := l.FindPostings()

		c.JSON(200, gin.H{
			"ok":       err == nil,
			"postings": results,
		})
	})

	r.GET("/accounts", func(c *gin.Context) {
		results, err := l.FindAccounts()

		c.JSON(200, gin.H{
			"ok":       err == nil,
			"accounts": results,
		})
	})

	r.GET("/accounts/:address", func(c *gin.Context) {
		res, err := l.GetAccount(c.Param("address"))

		c.JSON(200, gin.H{
			"ok":      err == nil,
			"account": res,
		})
	})

	h := &HttpAPI{
		engine: r,
	}

	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			go h.Start()

			return nil
		},
	})

	return h
}

func (h *HttpAPI) Start() {
	h.engine.Run("localhost:3068")
}
