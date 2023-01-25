package oidc

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/zitadel/oidc/pkg/client/rp"
	"github.com/zitadel/oidc/pkg/op"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const AuthorizeCallbackPath = "/authorize/callback"

func AddRoutes(router *mux.Router, provider op.OpenIDProvider, storage Storage, relyingParty rp.RelyingParty) {
	router.NewRoute().Path(AuthorizeCallbackPath).Queries("code", "{code}").
		Handler(authorizeCallbackHandler(provider, storage, relyingParty))
	router.NewRoute().Path(AuthorizeCallbackPath).Queries("error", "{error}").
		Handler(authorizeErrorHandler())

	oidcLibRouter := router.PathPrefix("/").Subrouter()
	oidcLibRouter.Use(func(handler http.Handler) http.Handler {
		// The otelmux middleware does not see matching route as it is matched in a subrouter
		// So the span name terminated with just "/"
		// This middleware make the hack
		// We can do this because url does not contain any dynamic variables.
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			currentSpan := trace.SpanFromContext(r.Context())
			currentSpan.SetName(r.URL.Path)
			currentSpan.SetAttributes(attribute.String("http.route", r.URL.Path))
			handler.ServeHTTP(w, r)
		})
	})
	oidcLibRouter.PathPrefix("/").Handler(provider.HttpHandler())
}
