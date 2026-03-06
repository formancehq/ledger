package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v3/logging"
)

// HTTPAuthMiddleware returns an HTTP middleware that validates JWT tokens,
// stores the claims in the request context, and expands scopes through the
// scope mapping. Public endpoints (/health, /debug/*) are skipped.
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
			if path == "/health" || path == "/livez" || path == "/readyz" || strings.HasPrefix(path, "/debug/") {
				next.ServeHTTP(w, r)

				return
			}

			token, err := bearerTokenFromHTTP(r)
			if err != nil {
				logHTTPAuthFailure(r, "", "missing_token", err)
				http.Error(w, err.Error(), http.StatusUnauthorized)

				return
			}

			keyID := extractKeyID(token)

			claims, err := validateToken(r.Context(), token, cfg)
			if err != nil {
				logHTTPAuthFailure(r, keyID, "invalid_token", err)
				http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)

				return
			}

			ctx := WithClaims(r.Context(), claims)

			// Expand scopes through the mapping and store in context
			effective := cfg.ScopeMapping.ExpandScopes(claims.Scopes)
			ctx = WithExpandedScopes(ctx, effective)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScope returns an HTTP middleware that checks the authenticated user
// has all the given granular scopes. Must be used after HTTPAuthMiddleware.
// When cfg.Enabled is false, it passes through.
func RequireScope(cfg AuthConfig, scopes ...Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)

				return
			}

			effective := ExpandedScopesFromContext(r.Context())
			if effective == nil {
				logHTTPAuthFailure(r, "", "missing_auth", errors.New("no expanded scopes in context"))
				http.Error(w, "missing authentication", http.StatusUnauthorized)

				return
			}

			if !HasScope(effective, scopes...) {
				logHTTPAuthFailure(r, "", "missing_scope", fmt.Errorf("required: %v", scopes))
				http.Error(w, "missing required scope", http.StatusForbidden)

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// logHTTPAuthFailure logs an authentication failure from an HTTP request with structured fields.
func logHTTPAuthFailure(r *http.Request, keyID, reason string, err error) {
	fields := map[string]any{
		"reason":     reason,
		"error":      err.Error(),
		"remoteAddr": r.RemoteAddr,
	}
	if keyID != "" {
		fields["keyId"] = keyID
	}

	logging.FromContext(r.Context()).WithFields(fields).Infof("auth failure")
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
