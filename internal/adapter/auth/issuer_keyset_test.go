package auth

import (
	"context"
	"crypto/rsa"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
)

func TestValidateToken_IssuerKeySetRoutesByIssuer(t *testing.T) {
	t.Parallel()

	const (
		issuerA = "https://issuer-a.example.com"
		issuerB = "https://issuer-b.example.com"
	)

	// Both key sets publish the same key ID, so only issuer routing tells them apart.
	keyA, keySetA := testKeyPair(t)
	keyB, keySetB := testKeyPair(t)
	cfg := AuthConfig{
		Enabled: true,
		KeySet:  NewIssuerKeySet(map[string]oidc.KeySet{issuerA: keySetA, issuerB: keySetB}),
		Issuers: []string{issuerA, issuerB},
	}

	token := func(key *rsa.PrivateKey, issuer string) string {
		claims := newTestClaims("ledger:read")
		claims.Issuer = issuer

		return signToken(t, key, claims)
	}

	for _, tc := range []struct {
		name    string
		token   string
		wantErr error
		wantMsg string
	}{
		{name: "first issuer", token: token(keyA, issuerA)},
		{name: "second issuer", token: token(keyB, issuerB)},
		{name: "second issuer signed by first issuer's key", token: token(keyA, issuerB), wantErr: oidc.ErrSignatureInvalid},
		{name: "first issuer signed by second issuer's key", token: token(keyB, issuerA), wantErr: oidc.ErrSignatureInvalid},
		{name: "untrusted issuer", token: token(keyA, "https://untrusted.example.com"), wantErr: oidc.ErrSignatureInvalid, wantMsg: "untrusted issuer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			claims, err := validateToken(context.Background(), tc.token, cfg)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.ErrorContains(t, err, tc.wantMsg)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, claims)
		})
	}
}

func TestValidateToken_RejectsIssuerOutsideTrustedList(t *testing.T) {
	t.Parallel()

	key, keySet := testKeyPair(t)
	cfg := AuthConfig{
		Enabled: true,
		KeySet:  keySet,
		Issuers: []string{"https://issuer-a.example.com", "https://issuer-b.example.com"},
	}

	claims := newTestClaims("ledger:read")
	claims.Issuer = "https://issuer-c.example.com"

	_, err := validateToken(context.Background(), signToken(t, key, claims), cfg)
	require.ErrorIs(t, err, oidc.ErrIssuerInvalid)
}
