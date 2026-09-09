package auth

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
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

func TestTokenCommands_Audience(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keyID, err := signing.GenerateKeyPair(dir)
	require.NoError(t, err)
	pubKey, err := signing.LoadPublicKeyFromFile(filepath.Join(dir, "pubkey.hex"))
	require.NoError(t, err)

	for _, command := range []struct {
		name   string
		newCmd func() *cobra.Command
		params func(*cobra.Command) (tokenParams, error)
	}{
		{name: "generate-token", newCmd: NewGenerateTokenCommand, params: tokenParamsFromFlags},
		{name: "login", newCmd: NewLoginCommand, params: resolveLoginParams},
	} {
		t.Run(command.name, func(t *testing.T) {
			t.Parallel()

			for _, audience := range []string{"", " \t", "https://ledger.example.com/prod"} {
				t.Run(audience, func(t *testing.T) {
					t.Parallel()

					cmd := command.newCmd()
					cmd.Flags().String("server", "https://ledger.example.com/prod", "")
					require.NoError(t, cmd.ParseFlags([]string{
						"--signing-key", filepath.Join(dir, "seed.hex"),
						"--key-id", keyID,
						"--subject", "test-bot",
					}))
					if audience != "" {
						require.NoError(t, cmd.Flags().Set("audience", audience))
					}
					params, err := command.params(cmd)
					if audience != "https://ledger.example.com/prod" {
						require.EqualError(t, err, "required flag \"audience\" must not be empty")

						return
					}
					require.NoError(t, err)

					token, err := signToken(params)
					require.NoError(t, err)
					parsed, err := jose.ParseSigned(token, []jose.SignatureAlgorithm{jose.EdDSA})
					require.NoError(t, err)
					payload, err := parsed.Verify(pubKey)
					require.NoError(t, err)
					var claims oidc.AccessTokenClaims
					require.NoError(t, json.Unmarshal(payload, &claims))
					require.Equal(t, []string{audience}, claims.GetAudience())
				})
			}
		})
	}
}

func TestSignToken_RejectsEmptyAudience(t *testing.T) {
	t.Parallel()

	for _, audience := range []string{"", " \t"} {
		t.Run(audience, func(t *testing.T) {
			t.Parallel()
			token, err := signToken(tokenParams{
				seed: make([]byte, ed25519.SeedSize), audience: audience, expiration: time.Hour,
			})
			require.EqualError(t, err, "required flag \"audience\" must not be empty")
			require.Empty(t, token)
		})
	}
}
