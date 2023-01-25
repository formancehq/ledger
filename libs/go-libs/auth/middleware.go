package auth

import (
	"net/http"

	"github.com/formancehq/go-libs/logging"
)

func Middleware(methods ...Method) func(handler http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ok := false
			for _, m := range methods {
				if m.IsMatching(r) {
					agent, err := m.Check(r)
					if err != nil {
						logging.GetLogger(r.Context()).WithFields(map[string]any{
							"err": err,
						}).Infof("Access denied")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					r = r.WithContext(WithAgent(r.Context(), agent))
					ok = true
					break
				}
			}
			if !ok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			h.ServeHTTP(w, r)
		})

	}
}

func NeedAllScopes(scopes ...string) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			agent := AgentFromContext(r.Context())
			if agent == nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		l:
			for _, scope := range scopes {
				for _, agentScope := range agent.GetScopes() {
					if agentScope == scope {
						continue l
					}
				}
				// Scope not found
				w.WriteHeader(http.StatusForbidden)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

func NeedOneOfScopes(scopes ...string) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			agent := AgentFromContext(r.Context())
			if agent == nil {
				w.WriteHeader(http.StatusForbidden)
				return
			}

			for _, scope := range scopes {
				for _, agentScope := range agent.GetScopes() {
					if agentScope == scope {
						h.ServeHTTP(w, r)
						return
					}
				}
			}
			w.WriteHeader(http.StatusForbidden)
		})
	}
}
