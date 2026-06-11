package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestResolveCallerIdentity_Unauthenticated(t *testing.T) {
	t.Parallel()

	require.Nil(t, ResolveCallerIdentity(context.Background()))
}

func TestResolveCallerIdentity_FromClaims_OIDC(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{
			Issuer:  "https://issuer.example.com",
			Subject: "user-1",
		},
	}
	ctx := WithClaims(context.Background(), claims)
	ctx = WithExpandedScopes(ctx, map[Scope]struct{}{
		ScopeTransactionsRead:  {},
		ScopeTransactionsWrite: {},
	})

	got := ResolveCallerIdentity(ctx)
	require.NotNil(t, got)
	require.Equal(t, "user-1", got.GetSubject())
	require.False(t, got.GetGod())
	require.Equal(t, "https://issuer.example.com", got.GetIssuer())
	// Scopes must be sorted for deterministic Raft serialization.
	require.Equal(t, []string{string(ScopeTransactionsRead), string(ScopeTransactionsWrite)}, got.GetScopes())
}

func TestResolveCallerIdentity_FromClaims_Ed25519_PrefersKeyID(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{
			Issuer:  "ignored-when-keyid-present",
			Subject: "service-acct",
		},
	}
	ctx := WithClaims(context.Background(), claims)
	ctx = WithKeyID(ctx, "ed25519-key-7")

	got := ResolveCallerIdentity(ctx)
	require.NotNil(t, got)
	require.Equal(t, "ed25519-key-7", got.GetKeyId())
	require.Empty(t, got.GetIssuer())
}

func TestResolveCallerIdentity_GodClaim(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{Subject: "admin"},
		Claims:      map[string]any{"god": true},
	}
	ctx := WithClaims(context.Background(), claims)

	got := ResolveCallerIdentity(ctx)
	require.NotNil(t, got)
	require.True(t, got.GetGod())
}

func TestResolveCallerIdentity_ForwardedShortCircuitsClaims(t *testing.T) {
	t.Parallel()

	// Local claims describe the peer (cluster-secret would normally yield no
	// claims, but we set them here to verify the forwarded slot takes priority
	// even when both are present).
	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{Subject: "peer-node"},
	}
	forwarded := &commonpb.CallerIdentity{
		Subject: "original-user",
		Scopes:  []string{"transactions:write"},
		Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
	}

	ctx := WithClaims(context.Background(), claims)
	ctx = WithForwardedCaller(ctx, forwarded)

	got := ResolveCallerIdentity(ctx)
	require.NotNil(t, got)
	require.Equal(t, "original-user", got.GetSubject(),
		"forwarded caller must win over local peer claims")
	require.Equal(t, "https://idp.example.com", got.GetIssuer())
}

func TestIsClusterInternal_DefaultsFalse(t *testing.T) {
	t.Parallel()

	require.False(t, IsClusterInternal(context.Background()))
}

func TestWithClusterInternal_RoundTrip(t *testing.T) {
	t.Parallel()

	ctx := WithClusterInternal(context.Background(), true)
	require.True(t, IsClusterInternal(ctx))

	ctx = WithClusterInternal(ctx, false)
	require.False(t, IsClusterInternal(ctx))
}

func TestForwardedCallerFromContext_DefaultsNil(t *testing.T) {
	t.Parallel()

	require.Nil(t, ForwardedCallerFromContext(context.Background()))
}

func TestWithForwardedCaller_RoundTrip(t *testing.T) {
	t.Parallel()

	identity := &commonpb.CallerIdentity{Subject: "abc"}
	ctx := WithForwardedCaller(context.Background(), identity)

	got := ForwardedCallerFromContext(ctx)
	require.Same(t, identity, got)
}
