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
