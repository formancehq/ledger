package auth

import (
	"context"
	"strings"

	"github.com/formancehq/go-libs/v3/oidc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthConfig holds the configuration for gRPC and HTTP authentication.
type AuthConfig struct {
	Enabled     bool
	KeySet      oidc.KeySet
	Issuer      string
	Service     string
	CheckScopes bool
}

// Authenticate validates the JWT from gRPC metadata and checks required scopes.
// If auth is disabled, returns the original context unchanged.
// Returns the context enriched with claims, or a gRPC status error.
func Authenticate(ctx context.Context, cfg AuthConfig, scopes ...Scope) (context.Context, error) {
	if !cfg.Enabled {
		return ctx, nil
	}

	token, err := bearerTokenFromContext(ctx)
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated, err.Error())
	}

	claims, err := validateToken(ctx, token, cfg.Issuer, cfg.KeySet)
	if err != nil {
		return ctx, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	if cfg.CheckScopes {
		for _, scope := range scopes {
			if !HasScope(claims.Scopes, scope, cfg.Service) {
				return ctx, status.Errorf(codes.PermissionDenied,
					"missing required scope %s", scope.WithService(cfg.Service))
			}
		}
	}

	return WithClaims(ctx, claims), nil
}

// bearerTokenFromContext extracts the Bearer token from gRPC incoming metadata.
func bearerTokenFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := values[0]
	if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		return "", status.Error(codes.Unauthenticated, "malformed authorization header")
	}

	return strings.TrimSpace(authHeader[7:]), nil
}

// validateToken validates a JWT token using the OIDC verification functions from go-libs.
func validateToken(ctx context.Context, token string, issuer string, keySet oidc.KeySet) (*oidc.AccessTokenClaims, error) {
	claims := &oidc.AccessTokenClaims{}

	decrypted, err := oidc.DecryptToken(token)
	if err != nil {
		return nil, err
	}

	payload, err := oidc.ParseToken(decrypted, claims)
	if err != nil {
		return nil, err
	}

	if err := oidc.CheckIssuer(claims, issuer); err != nil {
		return nil, err
	}

	if _, err := oidc.CheckSignature(ctx, decrypted, payload, []string{}, keySet); err != nil {
		return nil, err
	}

	if err := oidc.CheckExpiration(claims, 0); err != nil {
		return nil, err
	}

	return claims, nil
}
