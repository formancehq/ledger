package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/signing"
)

func TestGenerateToken_RoundTrip(t *testing.T) {
	t.Parallel()

	// Generate a keypair.
	dir := t.TempDir()
	keyID, err := signing.GenerateKeyPair(dir)
	require.NoError(t, err)

	// Load the keys.
	seed, err := signing.LoadSeedFromFile(dir + "/seed.hex")
	require.NoError(t, err)

	pubKey, err := signing.LoadPublicKeyFromFile(dir + "/pubkey.hex")
	require.NoError(t, err)

	privKey := ed25519.NewKeyFromSeed(seed)

	// Build and sign claims (mirrors what generate-token does).
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = "test-bot"
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}

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
	require.NotEmpty(t, token)

	// Verify the token with the public key.
	jwk := jose.JSONWebKey{
		Key:       pubKey,
		KeyID:     keyID,
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}
	keySet := oidc.NewStaticKeySet(jwk)

	parsedJWS, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.EdDSA})
	require.NoError(t, err)

	verifiedPayload, err := keySet.VerifySignature(context.Background(), parsedJWS)
	require.NoError(t, err)

	var verifiedClaims oidc.AccessTokenClaims

	err = json.Unmarshal(verifiedPayload, &verifiedClaims)
	require.NoError(t, err)
	assert.Equal(t, "test-bot", verifiedClaims.GetSubject())
	assert.Equal(t, oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}, verifiedClaims.Scopes)
}
