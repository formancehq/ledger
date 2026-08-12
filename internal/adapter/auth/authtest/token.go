// Package authtest provides token-minting helpers for tests that need to
// exercise authenticated requests.
//
// It deliberately does NOT import internal/adapter/auth: the auth package's own
// tests are in-package (package auth), so an authtest -> auth edge would be an
// import cycle in the test binary. Scopes are therefore plain strings here;
// callers convert with string(auth.ScopeLedgersRead).
package authtest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
)

// TestKeyID is the key ID carried by keys and tokens this package produces.
const TestKeyID = "test-key-id"

// KeyPair generates an RSA key pair and returns the private key together with a
// static JWKS KeySet that verifies tokens signed by it.
func KeyPair(t *testing.T) (*rsa.PrivateKey, oidc.KeySet) {
	t.Helper()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwk := jose.JSONWebKey{
		Key:       &privKey.PublicKey,
		KeyID:     TestKeyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}

	return privKey, oidc.NewStaticKeySet(jwk)
}

// Claims builds access-token claims for the given issuer, carrying the given
// token scopes and a one-hour validity window.
func Claims(issuer string, scopes ...string) *oidc.AccessTokenClaims {
	now := time.Now()

	claims := &oidc.AccessTokenClaims{}
	claims.Issuer = issuer
	claims.Subject = "test-user"
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(1 * time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)

	return claims
}

// SignToken serialises claims as an RS256 compact JWS signed by key.
func SignToken(t *testing.T, key *rsa.PrivateKey, claims *oidc.AccessTokenClaims) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       &jose.JSONWebKey{Key: key, KeyID: TestKeyID},
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
