package api

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

	cc := cors.DefaultConfig()
	cc.AllowAllOrigins = true
	cc.AllowCredentials = true
	cc.AddAllowHeaders("authorization")
	r.Use(cors.New(cc))

	r.Use(gin.Recovery())

	if auth := viper.Get("server.http.basic_auth"); auth != nil {
		segment := strings.Split(auth.(string), ":")

		r.Use(gin.BasicAuth(gin.Accounts{
			segment[0]: segment[1],
		}))
	}

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
				"ledgers": viper.Get("ledgers"),
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

		res := gin.H{
			"ok": err == nil,
		}

		if err != nil {
			res["err"] = err.Error()
		}

		c.JSON(200, res)
	})

	r.POST("/:ledger/script", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		var script core.Script
		c.ShouldBind(&script)

		err := l.(*ledger.Ledger).Execute(script)

		res := gin.H{
			"ok": err == nil,
		}

		if err != nil {
			err_str := err.Error()
			err_str = strings.ReplaceAll(err_str, "\n", "\r\n")
			payload, err := json.Marshal(gin.H{
				"error": err_str,
			})
			if err != nil {
				log.Fatal(err)
			}
			payload_b64 := base64.StdEncoding.EncodeToString([]byte(payload))
			link := fmt.Sprintf("https://play.numscript.org/?payload=%v", payload_b64)
			res["err"] = err_str
			res["details"] = link
		}

		c.JSON(200, res)
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

	r.POST("/:ledger/transactions/:id/metadata", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		var m core.Metadata
		c.ShouldBind(&m)

		err := l.(*ledger.Ledger).SaveMeta(
			"transaction",
			c.Param("id"),
			m,
		)

		res := gin.H{
			"ok": err == nil,
		}

		if err != nil {
			res["err"] = err.Error()
		}

		c.JSON(200, res)
	})

	r.POST("/:ledger/accounts/:id/metadata", func(c *gin.Context) {
		l, _ := c.Get("ledger")

		var m core.Metadata
		c.ShouldBind(&m)

		err := l.(*ledger.Ledger).SaveMeta(
			"account",
			c.Param("id"),
			m,
		)

		res := gin.H{
			"ok": err == nil,
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
