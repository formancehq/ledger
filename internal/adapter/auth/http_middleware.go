package auth

import (
	"errors"
	"net/http"
	"strings"
)

// HTTPAuthMiddleware returns an HTTP middleware that validates JWT tokens and
// stores the claims in the request context. It does NOT check scopes — use
// RequireScope for that.
// Public endpoints (/health, /debug/*) are skipped.
// When cfg.Enabled is false, requests pass through without authentication.
func HTTPAuthMiddleware(cfg AuthConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)
				return
			}

			// Public endpoints: no auth required
			path := strings.TrimPrefix(r.URL.Path, "/v2")
			if path == "/health" || strings.HasPrefix(path, "/debug/") {
				next.ServeHTTP(w, r)
				return
			}

			token, err := bearerTokenFromHTTP(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			claims, err := validateToken(r.Context(), token, cfg)
			if err != nil {
				http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)
				return
			}

			ctx := WithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScope returns an HTTP middleware that checks the authenticated user
// has all the given scopes. Must be used after HTTPAuthMiddleware.
// When cfg.Enabled is false or cfg.CheckScopes is false, it passes through.
func RequireScope(cfg AuthConfig, scopes ...Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled || !cfg.CheckScopes {
				next.ServeHTTP(w, r)
				return
			}

			claims := ClaimsFromContext(r.Context())
			if claims == nil {
				http.Error(w, "missing authentication", http.StatusUnauthorized)
				return
			}

			for _, scope := range scopes {
				if !HasScope([]string(claims.Scopes), scope, cfg.Service) {
					http.Error(w, "missing required scope "+scope.WithService(cfg.Service), http.StatusForbidden)
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

// bearerTokenFromHTTP extracts the Bearer token from the HTTP Authorization header.
func bearerTokenFromHTTP(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", errors.New("missing authorization header")
	}

	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return "", errors.New("malformed authorization header")
	}

	return strings.TrimSpace(authHeader[7:]), nil
}
