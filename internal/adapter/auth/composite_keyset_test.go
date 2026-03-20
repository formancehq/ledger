package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
)

func ed25519KeyPair(t *testing.T) (ed25519.PrivateKey, oidc.KeySet) {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	pub, ok := priv.Public().(ed25519.PublicKey)
	require.True(t, ok, "ed25519 private key must produce ed25519.PublicKey")

	jwk := jose.JSONWebKey{
		Key:       pub,
		KeyID:     "ed-test",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	return priv, oidc.NewStaticKeySet(jwk)
}

func signEdDSAToken(t *testing.T, privKey ed25519.PrivateKey, keyID string) string {
	t.Helper()

	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = "ed-user"
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(1 * time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read"}

	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.EdDSA,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: keyID},
	}, nil)
	require.NoError(t, err)

	jws, err := signer.Sign(payload)
	require.NoError(t, err)

	token, err := jws.CompactSerialize()
	require.NoError(t, err)

	return token
}

func TestCompositeKeySet_EdDSAToken(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519KeyPair(t)
	_, rsaKeySet := testKeyPair(t)

	composite := NewCompositeKeySet(edKeySet, rsaKeySet)

	token := signEdDSAToken(t, edPriv, "ed-test")
	jws, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.EdDSA})
	require.NoError(t, err)

	payload, err := composite.VerifySignature(context.Background(), jws)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestCompositeKeySet_RSAToken(t *testing.T) {
	t.Parallel()

	_, edKeySet := ed25519KeyPair(t)
	rsaPriv, rsaKeySet := testKeyPair(t)

	composite := NewCompositeKeySet(edKeySet, rsaKeySet)

	token := signToken(t, rsaPriv, newTestClaims("ledger:read"))
	jws, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.RS256})
	require.NoError(t, err)

	payload, err := composite.VerifySignature(context.Background(), jws)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestCompositeKeySet_NilOIDC(t *testing.T) {
	t.Parallel()

	edPriv, edKeySet := ed25519KeyPair(t)
	composite := NewCompositeKeySet(edKeySet, nil)

	token := signEdDSAToken(t, edPriv, "ed-test")
	jws, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.EdDSA})
	require.NoError(t, err)

	payload, err := composite.VerifySignature(context.Background(), jws)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestCompositeKeySet_NilEd25519(t *testing.T) {
	t.Parallel()

	rsaPriv, rsaKeySet := testKeyPair(t)
	composite := NewCompositeKeySet(nil, rsaKeySet)

	token := signToken(t, rsaPriv, newTestClaims("ledger:read"))
	jws, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.RS256})
	require.NoError(t, err)

	payload, err := composite.VerifySignature(context.Background(), jws)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)
}

func TestCompositeKeySet_AllNil(t *testing.T) {
	t.Parallel()

	composite := NewCompositeKeySet(nil, nil, nil)
	assert.Nil(t, composite)
}

func TestCompositeKeySet_ThreeKeySets(t *testing.T) {
	t.Parallel()

	// Create two Ed25519 key sets and one RSA key set.
	edPriv1, edKeySet1 := ed25519KeyPair(t)
	_, edKeySet2 := ed25519KeyPair(t) // different key, won't verify edPriv1 tokens
	rsaPriv, rsaKeySet := testKeyPair(t)

	composite := NewCompositeKeySet(edKeySet1, edKeySet2, rsaKeySet)

	// EdDSA token signed by edPriv1 should be verified by edKeySet1 (first in order).
	edToken := signEdDSAToken(t, edPriv1, "ed-test")
	edJWS, err := jose.ParseSigned(edToken, []jose.SignatureAlgorithm{jose.EdDSA})
	require.NoError(t, err)
	payload, err := composite.VerifySignature(context.Background(), edJWS)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)

	// RSA token should fall through to rsaKeySet (third).
	rsaToken := signToken(t, rsaPriv, newTestClaims("ledger:read"))
	rsaJWS, err := jose.ParseSigned(rsaToken, []jose.SignatureAlgorithm{jose.RS256})
	require.NoError(t, err)
	payload, err = composite.VerifySignature(context.Background(), rsaJWS)
	require.NoError(t, err)
	assert.NotEmpty(t, payload)
}
