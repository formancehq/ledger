package authorization

import (
	"errors"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/zitadel/oidc/pkg/op"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Invoke(func(router *mux.Router, o op.OpenIDProvider) error {
			return router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
				route.Handler(
					middleware(o)(route.GetHandler()),
				)
				return nil
			})
		}),
	)
}

var (
	ErrMissingAuthHeader   = errors.New("missing authorization header")
	ErrMalformedAuthHeader = errors.New("malformed authorization header")
	ErrVerifyAuthToken     = errors.New("could not verify access token")
)

func middleware(o op.OpenIDProvider) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if err := verifyAccessToken(r, o); err != nil {
					http.Error(w, err.Error(), http.StatusUnauthorized)
					return
				}

				h.ServeHTTP(w, r)
			})
	}
}
