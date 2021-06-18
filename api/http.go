package api

import (
	"context"
	_ "embed"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
	"numary.io/ledger/core"
	"numary.io/ledger/ledger"
	"numary.io/ledger/ledger/query"
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

	r.GET("/stats", func(c *gin.Context) {
		stats, err := l.Stats()

		c.JSON(200, gin.H{
			"ok":    err == nil,
			"stats": stats,
		})
	})

	r.GET("/transactions", func(c *gin.Context) {
		cursor, err := l.FindTransactions(
			query.After(c.Query("after")),
			query.Account(c.Query("account")),
		)

		c.JSON(200, gin.H{
			"ok":     err == nil,
			"cursor": cursor,
		})
	})

	r.POST("/transactions", func(c *gin.Context) {
		var t core.Transaction
		c.ShouldBind(&t)

		err := l.Commit(t)

		c.JSON(304, gin.H{
			"ok": err != nil,
		})
	})

	r.GET("/accounts", func(c *gin.Context) {
		cursor, err := l.FindAccounts(
			query.After(c.Query("after")),
		)

		c.JSON(200, gin.H{
			"ok":     err == nil,
			"cursor": cursor,
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
