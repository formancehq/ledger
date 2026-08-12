package authtest_test

import (
	"context"
	"testing"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/adapter/auth/authtest"
)

// TestSignToken_VerifiesAgainstKeySet proves a minted token is a structurally
// valid compact JWS that the paired KeySet accepts, which is what
// auth.validateToken requires (oidc.ParseToken + oidc.CheckSignature).
func TestSignToken_VerifiesAgainstKeySet(t *testing.T) {
	t.Parallel()

	key, keySet := authtest.KeyPair(t)
	token := authtest.SignToken(t, key, authtest.Claims("https://issuer.test", "ledger:LedgerRead"))

	claims := &oidc.AccessTokenClaims{}
	payload, err := oidc.ParseToken(token, claims)
	require.NoError(t, err)

	_, err = oidc.CheckSignature(context.Background(), token, payload, []string{string(jose.RS256)}, keySet)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, "https://issuer.test", claims.Issuer)
	require.Equal(t, oidc.SpaceDelimitedArray{"ledger:LedgerRead"}, claims.Scopes)
}
