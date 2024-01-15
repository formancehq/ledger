package auth

import (
	"net/http"
)

type Auth interface {
	Authenticate(w http.ResponseWriter, r *http.Request) (bool, error)
}

func Middleware(ja Auth) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authenticated, err := ja.Authenticate(w, r)
			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			if !authenticated {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			handler.ServeHTTP(w, r)
		})
	}
}
