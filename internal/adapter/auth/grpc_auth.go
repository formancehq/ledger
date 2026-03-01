package auth

import (
	"context"
	"strings"

	"github.com/formancehq/go-libs/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthConfig holds the configuration for gRPC and HTTP authentication.
type AuthConfig struct {
	Enabled              bool
	KeySet               oidc.KeySet
	Issuer               string
	Service              string
	CheckScopes          bool
	Ed25519AllowedScopes map[string][]string // keyID -> allowed scopes (nil = no Ed25519 auth)
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

	claims, err := validateToken(ctx, token, cfg)
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

// validateToken validates a JWT token. It supports both OIDC (RS256/ES256/PS256) and
// EdDSA tokens. For EdDSA tokens, the issuer check is skipped (self-signed) and
// key-level scope enforcement is applied when Ed25519AllowedScopes is configured.
func validateToken(ctx context.Context, token string, cfg AuthConfig) (*oidc.AccessTokenClaims, error) {
	claims := &oidc.AccessTokenClaims{}

	decrypted, err := oidc.DecryptToken(token)
	if err != nil {
		return nil, err
	}

	payload, err := oidc.ParseToken(decrypted, claims)
	if err != nil {
		return nil, err
	}

	// Accept EdDSA in addition to the default algorithms (RS256, ES256, PS256).
	supportedAlgs := []string{
		string(jose.RS256), string(jose.ES256), string(jose.PS256),
		string(jose.EdDSA),
	}
	sigAlg, err := oidc.CheckSignature(ctx, decrypted, payload, supportedAlgs, cfg.KeySet)
	if err != nil {
		return nil, err
	}

	if sigAlg == jose.EdDSA {
		// EdDSA tokens are self-signed: skip OIDC issuer check.
		// Enforce key-level scope restrictions if configured.
		if cfg.Ed25519AllowedScopes != nil {
			keyID := extractKeyID(decrypted)
			if err := enforceAllowedScopes(claims.Scopes, keyID, cfg.Ed25519AllowedScopes); err != nil {
				return nil, err
			}
		}
	} else {
		// OIDC token: verify issuer as before.
		if err := oidc.CheckIssuer(claims, cfg.Issuer); err != nil {
			return nil, err
		}
	}

	if err := oidc.CheckExpiration(claims, 0); err != nil {
		return nil, err
	}

	return claims, nil
}

// extractKeyID extracts the kid from a compact JWS token header.
func extractKeyID(token string) string {
	jws, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.EdDSA, jose.RS256, jose.ES256, jose.PS256})
	if err != nil || len(jws.Signatures) == 0 {
		return ""
	}
	return jws.Signatures[0].Header.KeyID
}
