package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// HTTPAuthMiddleware returns an HTTP middleware that validates JWT tokens,
// stores the claims in the request context, and expands scopes through the
// scope mapping. Health endpoints (/health, /livez, /readyz) are skipped.
//
// Token handling:
//   - Missing Authorization header → no error; the request continues with the
//     "anonymous" scopes (cfg.ScopeMapping["anonymous"]). Whether the request
//     ultimately succeeds is decided by RequireScope downstream.
//   - Authorization header present but invalid (bad signature, expired,
//     malformed) → 401. A broken token is a client error.
//   - Valid token → effective scopes = expansion of the token's scopes.
//
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
			if path == "/health" || path == "/livez" || path == "/readyz" || path == "/_info" {
				next.ServeHTTP(w, r)

				return
			}

			token, hasToken := bearerTokenFromHTTP(r)
			if !hasToken {
				// No bearer token: fall back to anonymous scopes. The actual
				// authorization decision happens in RequireScope (or in the
				// per-Request loop of Apply / bulk handlers).
				ctx := WithExpandedScopes(r.Context(), cfg.ScopeMapping.AnonymousScopes())
				ctx = WithAuthPresented(ctx, false)
				next.ServeHTTP(w, r.WithContext(ctx))

				return
			}

			keyID := extractKeyID(token)

			ctx := WithKeyID(r.Context(), keyID)

			claims, err := validateToken(ctx, token, cfg)
			if err != nil {
				logHTTPAuthFailure(r, keyID, "invalid_token", err)
				http.Error(w, "invalid token: "+err.Error(), http.StatusUnauthorized)

				return
			}

			ctx = WithClaims(ctx, claims)

			// Expand scopes through the mapping and store in context.
			// God-mode tokens get all granular scopes.
			god := isGodMode(claims)
			trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("auth.god_mode", god))

			var effective map[Scope]struct{}
			if god {
				effective = allScopes()
			} else {
				effective = cfg.ScopeMapping.ExpandScopes(claims.Scopes)
			}

			ctx = WithExpandedScopes(ctx, effective)
			ctx = WithAuthPresented(ctx, true)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScope returns an HTTP middleware that checks the authenticated user
// has all the given granular scopes. Must be used after HTTPAuthMiddleware.
// When cfg.Enabled is false, it passes through.
//
// Status code semantics on failure:
//   - 401 Unauthorized: no bearer token was presented (anonymous request) and
//     the anonymous scopes are insufficient. The client should authenticate.
//   - 403 Forbidden: a valid bearer token was presented but lacks the required
//     scope. Re-authenticating with the same identity will not help.
func RequireScope(cfg AuthConfig, scopes ...Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !cfg.Enabled {
				next.ServeHTTP(w, r)

				return
			}

			effective := ExpandedScopesFromContext(r.Context())
			if HasScope(effective, scopes...) {
				next.ServeHTTP(w, r)

				return
			}

			if AuthPresentedFromContext(r.Context()) {
				logHTTPAuthFailure(r, "", "missing_scope", fmt.Errorf("required: %v", scopes))
				http.Error(w, "missing required scope", http.StatusForbidden)

				return
			}

			logHTTPAuthFailure(r, "", "missing_auth", errors.New("anonymous scopes insufficient"))
			http.Error(w, "missing authentication", http.StatusUnauthorized)
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

// bearerTokenFromHTTP extracts the Bearer token from the HTTP Authorization
// header. The boolean return is false when the header is missing or does not
// carry the "Bearer " prefix — both cases are treated as "no token presented"
// rather than "invalid token", because they cannot be distinguished from a
// caller that simply chose not to authenticate.
func bearerTokenFromHTTP(r *http.Request) (string, bool) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", false
	}

	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return "", false
	}

	return strings.TrimSpace(authHeader[7:]), true
}
