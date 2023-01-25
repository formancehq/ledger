package oauth2introspect

import (
	"net/http"
	"strings"
)

func NewMiddleware(i *Introspecter) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(strings.ToUpper(authHeader), "BEARER ") {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			bearer := authHeader[7:]
			active, err := i.Introspect(r.Context(), bearer)
			if err != nil {
				panic(err)
			}

			if !active {
				w.WriteHeader(http.StatusUnauthorized)
			}

			handler.ServeHTTP(w, r)
		})
	}
}
