package api

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

type HttpAPI struct {
	addr   string
	engine *gin.Engine
}

func NewHttpAPI(lc fx.Lifecycle, resolver *ledger.Resolver) *HttpAPI {
	gin.SetMode(gin.ReleaseMode)

	cc := cors.DefaultConfig()
	cc.AllowAllOrigins = true
	cc.AllowCredentials = true
	cc.AddAllowHeaders("authorization")

	router := Router(cc, resolver)

	h := &HttpAPI{
		engine: router,
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
