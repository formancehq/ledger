package auth

import (
	"context"
	"fmt"
	"strings"

	jose "github.com/go-jose/go-jose/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

var authTracer = otel.Tracer("auth")

// AuthConfig holds the configuration for gRPC and HTTP authentication.
type AuthConfig struct {
	Enabled              bool
	KeySet               oidc.KeySet
	Issuer               string
	Service              string
	ScopeMapping         ScopeMapping
	Ed25519AllowedScopes map[string][]string // keyID -> allowed scopes (nil = no Ed25519 auth)
	Ed25519GodKeys       map[string]bool     // keyID -> true if the key can emit god-mode tokens
	ClusterSecret        string              // shared secret for inter-node auth bypass (empty = disabled)
}

// Authenticate validates the JWT from gRPC metadata and checks required scopes.
// If auth is disabled, returns the original context unchanged.
// Returns the context enriched with claims and expanded scopes, or a gRPC status error.
func Authenticate(ctx context.Context, cfg AuthConfig, scopes ...Scope) (context.Context, error) {
	ctx, span := authTracer.Start(ctx, "auth.authenticate")
	defer span.End()

	if !cfg.Enabled {
		span.SetAttributes(attribute.Bool("auth.enabled", false))

		return ctx, nil
	}

	token, err := bearerTokenFromContext(ctx)
	if err != nil {
		logAuthFailure(ctx, "", "missing_token", err)

		return ctx, status.Error(codes.Unauthenticated, err.Error())
	}

	// Fast path: cluster-internal shared secret bypasses JWT validation.
	// Grant all granular scopes so that per-request scope checks (e.g. in Apply)
	// succeed when a follower forwards a request to the leader.
	if cfg.ClusterSecret != "" && token == cfg.ClusterSecret {
		span.SetAttributes(attribute.Bool("auth.cluster_internal", true))

		return WithExpandedScopes(ctx, allScopes()), nil
	}

	keyID := extractKeyID(token)

	claims, err := validateToken(ctx, token, cfg)
	if err != nil {
		logAuthFailure(ctx, keyID, "invalid_token", err)

		return ctx, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	ctx = WithClaims(ctx, claims)

	var effective map[Scope]struct{}
	if isGodMode(claims) {
		effective = allScopes()
	} else {
		effective = cfg.ScopeMapping.ExpandScopes(claims.Scopes)
	}

	ctx = WithExpandedScopes(ctx, effective)

	if !HasScope(effective, scopes...) {
		logAuthFailure(ctx, keyID, "missing_scope", fmt.Errorf("required: %v, have: %v", scopes, claims.Scopes))

		return ctx, status.Errorf(codes.PermissionDenied,
			"missing required scope (required: %v)", scopes)
	}

	return ctx, nil
}

// isGodMode checks whether the token contains the custom "god": true claim,
// which grants all granular scopes regardless of what scopes the token carries.
func isGodMode(claims *oidc.AccessTokenClaims) bool {
	god, ok := claims.Claims["god"]
	if !ok {
		return false
	}

	godBool, ok := god.(bool)

	return ok && godBool
}

// allScopes returns a set containing every granular scope.
func allScopes() map[Scope]struct{} {
	result := make(map[Scope]struct{}, len(AllGranularScopes))
	for s := range AllGranularScopes {
		result[s] = struct{}{}
	}

	return result
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

			err := enforceAllowedScopes(claims.Scopes, keyID, cfg.Ed25519AllowedScopes, cfg.Ed25519GodKeys)
			if err != nil {
				return nil, err
			}

			// If the token claims god mode, verify the signing key is allowed.
			if isGodMode(claims) {
				if err := enforceGodClaim(keyID, cfg.Ed25519GodKeys); err != nil {
					return nil, err
				}
			}
		}
	} else {
		// OIDC token: verify issuer as before.
		err := oidc.CheckIssuer(claims, cfg.Issuer)
		if err != nil {
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

// logAuthFailure logs an authentication or authorization failure with structured fields
// and records it on the current OpenTelemetry span. Setting the span status to Error
// ensures the ErrorAwareSamplingExporter always exports auth failures regardless of
// sampling ratio.
func logAuthFailure(ctx context.Context, keyID, reason string, err error) {
	fields := map[string]any{
		"reason": reason,
		"error":  err.Error(),
	}
	if keyID != "" {
		fields["keyId"] = keyID
	}

	var remoteAddr string
	if p, ok := peer.FromContext(ctx); ok {
		remoteAddr = p.Addr.String()
		fields["remoteAddr"] = remoteAddr
	}

	logging.FromContext(ctx).WithFields(fields).Infof("auth failure")

	// Record on the OTEL span so auth failures are always exported (error-aware sampling).
	span := trace.SpanFromContext(ctx)

	attrs := []attribute.KeyValue{
		attribute.String("auth.failure.reason", reason),
		attribute.String("auth.failure.error", err.Error()),
	}
	if keyID != "" {
		attrs = append(attrs, attribute.String("auth.key_id", keyID))
	}

	if remoteAddr != "" {
		attrs = append(attrs, attribute.String("auth.remote_addr", remoteAddr))
	}

	span.SetAttributes(attrs...)
	span.RecordError(err)
	span.SetStatus(otelcodes.Error, "auth failure: "+reason)
}
