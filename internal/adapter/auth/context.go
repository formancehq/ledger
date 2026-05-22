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
// Returns nil if no claims are present (e.g., auth disabled).
func ClaimsFromContext(ctx context.Context) *oidc.AccessTokenClaims {
	claims, _ := ctx.Value(contextKey{}).(*oidc.AccessTokenClaims)

	return claims
}

type keyIDKey struct{}

// WithKeyID returns a new context with the given JWT key ID (kid header) attached.
func WithKeyID(ctx context.Context, keyID string) context.Context {
	return context.WithValue(ctx, keyIDKey{}, keyID)
}

// KeyIDFromContext extracts the JWT key ID from the context.
// Returns an empty string if no key ID is present.
func KeyIDFromContext(ctx context.Context) string {
	keyID, _ := ctx.Value(keyIDKey{}).(string)

	return keyID
}
