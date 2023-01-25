package api

import (
	"context"
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/health"
	"github.com/gorilla/mux"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
	"go.uber.org/fx"
)

func CreateRootRouter() *mux.Router {
	rootRouter := mux.NewRouter()
	rootRouter.Use(otelmux.Middleware("auth"))
	rootRouter.Use(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			handler.ServeHTTP(w, r)
		})
	})
	return rootRouter
}

func addInfoRoute(router *mux.Router, serviceInfo api.ServiceInfo) {
	router.Path("/_info").Methods(http.MethodGet).HandlerFunc(api.InfoHandler(serviceInfo))
}

func Module(addr string, serviceInfo api.ServiceInfo) fx.Option {
	return fx.Options(
		health.ProvideHealthCheck(delegatedOIDCServerAvailability),
		health.Module(),
		fx.Supply(serviceInfo),
		fx.Provide(CreateRootRouter),
		fx.Invoke(func(lc fx.Lifecycle, r *mux.Router, healthController *health.HealthController) {
			finalRouter := mux.NewRouter()
			finalRouter.Path("/_healthcheck").HandlerFunc(healthController.Check)
			finalRouter.PathPrefix("/").Handler(r)
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return StartServer(ctx, addr, finalRouter)
				},
			})
		}),
		fx.Invoke(
			addInfoRoute,
			addClientRoutes,
			addScopeRoutes,
			addUserRoutes,
		),
	)
}
