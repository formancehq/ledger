package api

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

type HttpAPI struct {
	addr   string
	engine *gin.Engine
}

func NewHttpAPI(lc fx.Lifecycle, resolver *ledger.Resolver) *HttpAPI {
	gin.SetMode(gin.ReleaseMode)

	r := gin.Default()
	r.Use(cors.Default())
	r.Use(gin.Recovery())

	r.Use(func(c *gin.Context) {
		name := c.Param("ledger")

		if name == "" {
			return
		}

		l, err := resolver.GetLedger(name)

		if err != nil {
			c.JSON(400, gin.H{
				"ok":  false,
				"err": err.Error(),
			})
		}

		c.Set("ledger", l)
	})

	r.GET("/_info", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"server":  "numary-ledger",
			"version": "1.0.0-alpha.1",
			"config": gin.H{
				"storage": gin.H{
					"driver": viper.Get("storage.driver"),
				},
			},
		})
	})

	r.GET("/:ledger/stats", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		stats, err := l.(*ledger.Ledger).Stats()

		c.JSON(200, gin.H{
			"ok":    err == nil,
			"stats": stats,
		})
	})

	r.GET("/:ledger/transactions", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		cursor, err := l.(*ledger.Ledger).FindTransactions(
			query.After(c.Query("after")),
			query.Account(c.Query("account")),
		)

		c.JSON(200, gin.H{
			"ok":     err == nil,
			"cursor": cursor,
			"err":    err,
		})
	})

	r.POST("/:ledger/transactions", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		var t core.Transaction
		c.ShouldBind(&t)

		err := l.(*ledger.Ledger).Commit([]core.Transaction{t})

		c.JSON(200, gin.H{
			"ok": err == nil,
		})
	})

	r.POST("/:ledger/script", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		var script core.Script
		c.ShouldBind(&script)

		err := l.(*ledger.Ledger).Execute(script)

		c.JSON(200, gin.H{
			"ok": err == nil,
		})
	})

	r.GET("/:ledger/accounts", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		cursor, err := l.(*ledger.Ledger).FindAccounts(
			query.After(c.Query("after")),
		)

		res := gin.H{
			"ok":     err == nil,
			"cursor": cursor,
		}

		if err != nil {
			res["err"] = err.Error()
		}

		c.JSON(200, res)
	})

	r.GET("/:ledger/accounts/:address", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		acc, err := l.(*ledger.Ledger).GetAccount(c.Param("address"))

		res := gin.H{
			"ok":      err == nil,
			"account": acc,
		}

		if err != nil {
			res["err"] = err.Error()
		}

		c.JSON(200, res)
	})

	h := &HttpAPI{
		engine: r,
		addr:   viper.GetString("server.http.bind_address"),
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
