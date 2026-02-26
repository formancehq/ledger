package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testIssuer = "https://test-issuer.example.com"

// testKeyPair generates an RSA key pair and returns both private key and a static JWKS KeySet.
func testKeyPair(t *testing.T) (*rsa.PrivateKey, oidc.KeySet) {
	t.Helper()
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwk := jose.JSONWebKey{
		Key:       &privKey.PublicKey,
		KeyID:     "test-key-id",
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}
	return privKey, oidc.NewStaticKeySet(jwk)
}

// signToken creates a signed JWT with the given claims.
func signToken(t *testing.T, privKey *rsa.PrivateKey, claims *oidc.AccessTokenClaims) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: "test-key-id"},
	}, nil)
	require.NoError(t, err)

	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	jws, err := signer.Sign(payload)
	require.NoError(t, err)

	token, err := jws.CompactSerialize()
	require.NoError(t, err)
	return token
}

func newTestClaims(scopes ...string) *oidc.AccessTokenClaims {
	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Issuer = testIssuer
	claims.Subject = "test-user"
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(1 * time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)
	return claims
}

func ctxWithBearer(token string) context.Context {
	md := metadata.Pairs("authorization", "Bearer "+token)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestAuthenticate_Disabled(t *testing.T) {
	t.Parallel()

	ctx, err := Authenticate(context.Background(), AuthConfig{Enabled: false}, ScopeRead)
	require.NoError(t, err)
	require.NotNil(t, ctx)
}

func TestAuthenticate_NoScopes(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	// Authenticate with no required scopes (public endpoint)
	token := signToken(t, privKey, newTestClaims())
	ctx := ctxWithBearer(token)

	newCtx, err := Authenticate(ctx, cfg)
	require.NoError(t, err)
	claims := ClaimsFromContext(newCtx)
	require.NotNil(t, claims)
	assert.Equal(t, "test-user", claims.GetSubject())
}

func TestAuthenticate_MissingToken(t *testing.T) {
	t.Parallel()

	_, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	// No authorization header
	_, err := Authenticate(context.Background(), cfg, ScopeRead)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
}

func TestAuthenticate_ValidToken(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	token := signToken(t, privKey, newTestClaims("ledger:read"))
	ctx := ctxWithBearer(token)

	newCtx, err := Authenticate(ctx, cfg, ScopeRead)
	require.NoError(t, err)
	claims := ClaimsFromContext(newCtx)
	require.NotNil(t, claims)
	assert.Equal(t, "test-user", claims.GetSubject())
}

func TestAuthenticate_WrongScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	// Token has only write scope, but we require read
	token := signToken(t, privKey, newTestClaims("ledger:write"))
	ctx := ctxWithBearer(token)

	_, err := Authenticate(ctx, cfg, ScopeRead)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, st.Code())
}

func TestAuthenticate_ExpiredToken(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: false,
	}

	claims := newTestClaims("ledger:read")
	pastTime := time.Now().Add(-1 * time.Hour)
	claims.Expiration = oidc.FromTime(oidc.Time(pastTime.Unix()).AsTime())

	token := signToken(t, privKey, claims)
	ctx := ctxWithBearer(token)

	_, err := Authenticate(ctx, cfg, ScopeRead)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
}

func TestAuthenticate_ScopesNotChecked(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: false,
	}

	// Token has no scopes, but CheckScopes is false
	token := signToken(t, privKey, newTestClaims())
	ctx := ctxWithBearer(token)

	newCtx, err := Authenticate(ctx, cfg, ScopeRead)
	require.NoError(t, err)
	require.NotNil(t, newCtx)
}

func TestAuthenticate_WriteScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	token := signToken(t, privKey, newTestClaims("ledger:write"))
	ctx := ctxWithBearer(token)

	newCtx, err := Authenticate(ctx, cfg, ScopeWrite)
	require.NoError(t, err)
	require.NotNil(t, newCtx)
}

func TestAuthenticate_AdminScope(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: true,
	}

	token := signToken(t, privKey, newTestClaims("ledger:admin"))
	ctx := ctxWithBearer(token)

	newCtx, err := Authenticate(ctx, cfg, ScopeAdmin)
	require.NoError(t, err)
	require.NotNil(t, newCtx)
}

func TestAuthenticate_WrongIssuer(t *testing.T) {
	t.Parallel()

	privKey, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled:     true,
		KeySet:      keySet,
		Issuer:      testIssuer,
		Service:     "ledger",
		CheckScopes: false,
	}

	claims := newTestClaims("ledger:read")
	claims.Issuer = "https://wrong-issuer.example.com"

	token := signToken(t, privKey, claims)
	ctx := ctxWithBearer(token)

	_, err := Authenticate(ctx, cfg, ScopeRead)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unauthenticated, st.Code())
}
