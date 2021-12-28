package api

import (
	_ "embed"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/api/routes"
	"net/http"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
)

var Module = fx.Options(
	middlewares.Module,
	routes.Module,
	controllers.Module,
)

// API struct
type API struct {
	engine *gin.Engine
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.engine.ServeHTTP(w, r)
}

// NewAPI
func NewAPI(
	routes *routes.Routes,
) *API {
	gin.SetMode(gin.ReleaseMode)

	cc := cors.DefaultConfig()
	cc.AllowAllOrigins = true
	cc.AllowCredentials = true
	cc.AddAllowHeaders("authorization")

	h := &API{
		engine: routes.Engine(cc),
	}

	return h
}
