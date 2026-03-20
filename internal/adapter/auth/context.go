package auth

import (
	"context"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
)

type contextKey struct{}

// WithClaims returns a new context with the given access token claims attached.
func WithClaims(ctx context.Context, claims *oidc.AccessTokenClaims) context.Context {
	return context.WithValue(ctx, contextKey{}, claims)
}

// ClaimsFromContext extracts the access token claims from the context.
// Returns nil if no claims are present (e.g., auth disabled or unauthenticated endpoint).
func ClaimsFromContext(ctx context.Context) *oidc.AccessTokenClaims {
	claims, _ := ctx.Value(contextKey{}).(*oidc.AccessTokenClaims)

	return claims
}

// SubjectFromContext extracts the JWT subject from the context.
// Returns empty string if no claims are present.
func SubjectFromContext(ctx context.Context) string {
	claims := ClaimsFromContext(ctx)
	if claims == nil {
		return ""
	}

	return claims.GetSubject()
}
