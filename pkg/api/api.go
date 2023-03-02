package api

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/middlewares"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/go-chi/chi/v5"
	"go.uber.org/fx"
)

type API struct {
	handler chi.Router
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.handler.ServeHTTP(w, r)
}

func NewAPI(routes *routes.Routes) *API {
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
		health.Module(),
	)
}
