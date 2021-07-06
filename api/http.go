package api

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/config"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
	"go.uber.org/fx"
)

type HttpAPI struct {
	addr   string
	engine *gin.Engine
}

func NewHttpAPI(lc fx.Lifecycle, l *ledger.Ledger, c config.Config) *HttpAPI {
	r := gin.Default()

	r.Use(cors.Default())

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

		err := l.Commit([]core.Transaction{t})

		c.JSON(200, gin.H{
			"ok":  err == nil,
			"err": err.Error(),
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
		addr:   c.Server.Http.BindAddress,
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
	h.engine.Run(h.addr)
}
