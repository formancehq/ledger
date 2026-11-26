package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHTTPAuthenticatedCallerUsesVerifiedCredentialSource(t *testing.T) {
	t.Parallel()

	for _, algorithm := range []jose.SignatureAlgorithm{jose.RS256, jose.ES256, jose.PS256, jose.EdDSA} {
		t.Run(string(algorithm), func(t *testing.T) {
			t.Parallel()

			const keyID = "signing-key"
			var privateKey, publicKey any
			switch algorithm {
			case jose.RS256, jose.PS256:
				key, _ := testKeyPair(t)
				privateKey, publicKey = key, &key.PublicKey
			case jose.ES256:
				key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
				require.NoError(t, err)
				privateKey, publicKey = key, &key.PublicKey
			case jose.EdDSA:
				key, _ := ed25519TestKeyPair(t, keyID)
				privateKey, publicKey = key, key.Public()
			default:
				t.Fatalf("unsupported test algorithm: %s", algorithm)
			}
			cfg := testAuthConfig(t, oidc.NewStaticKeySet(jose.JSONWebKey{
				Key: publicKey, KeyID: keyID, Algorithm: string(algorithm), Use: "sig",
			}))
			cfg.Ed25519AllowedScopes = map[string][]string{keyID: {string(ScopeLedgersWrite)}}

			// Static credentials may contain an issuer too; that claim must not
			// override the mechanism that successfully verified the token.
			claims := newTestClaims(string(ScopeLedgersWrite))
			payload, err := json.Marshal(claims)
			require.NoError(t, err)
			signer, err := jose.NewSigner(jose.SigningKey{
				Algorithm: algorithm,
				Key:       &jose.JSONWebKey{Key: privateKey, KeyID: keyID},
			}, nil)
			require.NoError(t, err)
			signed, err := signer.Sign(payload)
			require.NoError(t, err)
			token, err := signed.CompactSerialize()
			require.NoError(t, err)

			var captured *commonpb.CallerSnapshot
			inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				caller, err := ResolveCallerAttribution(r.Context())
				require.NoError(t, err)
				captured = caller.Snapshot()
				w.WriteHeader(http.StatusCreated)
			})
			handler := HTTPAuthMiddleware(cfg)(RequireScope(cfg, ScopeLedgersWrite)(inner))
			r := httptest.NewRequest(http.MethodPost, "/v3/new-ledger", nil)
			// Credential evaluation must replace attribution inherited from an
			// earlier context with the current verified token's source.
			r = r.WithContext(WithKeyID(context.Background(), "previous-key"))
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)

			require.Equal(t, http.StatusCreated, w.Code)
			require.NotNil(t, captured.GetAuthenticated())
			identity := captured.GetAuthenticated().GetIdentity()
			require.Equal(t, "test-user", identity.GetSubject())
			require.Equal(t, []string{string(ScopeLedgersWrite)}, captured.GetAuthenticated().GetScopes())
			if algorithm == jose.EdDSA {
				require.Equal(t, &commonpb.CallerIdentity_KeyId{KeyId: keyID}, identity.GetSource())
			} else {
				require.Equal(t, &commonpb.CallerIdentity_Issuer{Issuer: testIssuer}, identity.GetSource())
			}
		})
	}
}
