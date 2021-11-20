package api

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/api/middlewares"
	"github.com/numary/ledger/api/routes"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

var Module = fx.Options(
	middlewares.Module,
	routes.Module,
	controllers.Module,
)

// API struct
type API struct {
	addr   string
	engine *gin.Engine
}

// NewAPI
func NewAPI(
	lc fx.Lifecycle,
	routes *routes.Routes,
) *API {
	gin.SetMode(gin.ReleaseMode)

	cc := cors.DefaultConfig()
	cc.AllowAllOrigins = true
	cc.AllowCredentials = true
	cc.AddAllowHeaders("authorization")

	h := &API{
		engine: routes.Engine(cc),
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

// Start
func (h *API) Start() {
	h.engine.Run(h.addr)
}
