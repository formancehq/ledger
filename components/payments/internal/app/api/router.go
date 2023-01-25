package api

import (
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/payments/internal/app/integration"
	"github.com/formancehq/payments/internal/app/models"
	"github.com/formancehq/payments/internal/app/storage"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gorilla/mux/otelmux"
)

func httpRouter(store *storage.Storage, serviceInfo api.ServiceInfo, connectorHandlers []connectorHandler) (*mux.Router, error) {
	rootMux := mux.NewRouter()

	if viper.GetBool(otelTracesFlag) {
		rootMux.Use(otelmux.Middleware(serviceName))
	}

	rootMux.Use(recoveryHandler(httpRecoveryFunc))
	rootMux.Use(httpCorsHandler())
	rootMux.Use(httpServeFunc)

	rootMux.Path("/_health").Handler(healthHandler(store))
	rootMux.Path("/_live").Handler(liveHandler())
	rootMux.Path("/_info").Handler(api.InfoHandler(serviceInfo))

	authGroup := rootMux.Name("authenticated").Subrouter()

	if methods := sharedAuthMethods(); len(methods) > 0 {
		authGroup.Use(auth.Middleware(methods...))
	}

	authGroup.Path("/payments").Methods(http.MethodGet).Handler(listPaymentsHandler(store))
	authGroup.Path("/payments/{paymentID}").Methods(http.MethodGet).Handler(readPaymentHandler(store))

	authGroup.Path("/accounts").Methods(http.MethodGet).Handler(listAccountsHandler(store))

	authGroup.HandleFunc("/connectors", readConnectorsHandler(store))

	connectorGroup := authGroup.PathPrefix("/connectors").Subrouter()

	connectorGroup.Path("/configs").Handler(connectorConfigsHandler())

	// TODO: It's not ideal to define it explicitly here
	// Refactor it when refactoring the HTTP lib.
	connectorGroup.Path("/stripe/transfers").Methods(http.MethodPost).
		Handler(handleStripeTransfers(store))

	for _, h := range connectorHandlers {
		connectorGroup.PathPrefix("/" + h.Provider.String()).Handler(
			http.StripPrefix("/connectors", h.Handler))

		connectorGroup.PathPrefix("/" + h.Provider.StringLower()).Handler(
			http.StripPrefix("/connectors", h.Handler))
	}

	return rootMux, nil
}

func connectorRouter[Config models.ConnectorConfigObject](
	provider models.ConnectorProvider,
	manager *integration.ConnectorManager[Config],
) *mux.Router {
	r := mux.NewRouter()

	addRoute(r, provider, "", http.MethodPost, install(manager))
	addRoute(r, provider, "", http.MethodDelete, uninstall(manager))
	addRoute(r, provider, "/config", http.MethodGet, readConfig(manager))
	addRoute(r, provider, "/reset", http.MethodPost, reset(manager))
	addRoute(r, provider, "/tasks", http.MethodGet, listTasks(manager))
	addRoute(r, provider, "/tasks/{taskID}", http.MethodGet, readTask(manager))

	return r
}

func addRoute(r *mux.Router, provider models.ConnectorProvider, path, method string, handler http.Handler) {
	r.Path("/" + provider.String() + path).Methods(method).Handler(handler)

	r.Path("/" + provider.StringLower() + path).Methods(method).Handler(handler)
}
