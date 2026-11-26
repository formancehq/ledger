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

// WithKeyID attaches the signing key ID from validated static credentials.
// OIDC credentials must set an empty value so a JWT kid cannot replace their issuer.
func WithKeyID(ctx context.Context, keyID string) context.Context {
	return context.WithValue(ctx, keyIDKey{}, keyID)
}

// KeyIDFromContext extracts the validated static credential's signing key ID.
// Returns an empty string for OIDC or when no static key ID is present.
func KeyIDFromContext(ctx context.Context) string {
	keyID, _ := ctx.Value(keyIDKey{}).(string)

	return keyID
}
