package api

import (
	_ "embed"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/api/routes"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
)

// API struct
type API struct {
	handler *gin.Engine
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

// NewAPI
func NewAPI(
	routes *routes.Routes,
) *API {
	gin.SetMode(gin.ReleaseMode)
	h := &API{
		handler: routes.Engine(),
	}
	return h
}

type Config struct {
	StorageDriver string
	Version       string
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		controllers.ProvideVersion(func() string {
			return cfg.Version
		}),
		middlewares.Module,
		routes.Module,
		controllers.Module,
		fx.Provide(NewAPI),
	)
}
