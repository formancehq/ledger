package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestResolveCallerSnapshot_Unauthenticated(t *testing.T) {
	t.Parallel()

	require.Nil(t, ResolveCallerSnapshot(context.Background()))
}

func TestResolveCallerSnapshot_FromClaims_OIDC(t *testing.T) {
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

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, "user-1", got.GetIdentity().GetSubject())
	require.False(t, got.GetGod())
	require.Equal(t, "https://issuer.example.com", got.GetIdentity().GetIssuer())
	// Scopes must be sorted for deterministic Raft serialization.
	require.Equal(t, []string{string(ScopeTransactionsRead), string(ScopeTransactionsWrite)}, got.GetScopes())
}

func TestResolveCallerSnapshot_FromClaims_Ed25519_PrefersKeyID(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{
			Issuer:  "ignored-when-keyid-present",
			Subject: "service-acct",
		},
	}
	ctx := WithClaims(context.Background(), claims)
	ctx = WithKeyID(ctx, "ed25519-key-7")

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, "ed25519-key-7", got.GetIdentity().GetKeyId())
	require.Empty(t, got.GetIdentity().GetIssuer())
}

func TestResolveCallerSnapshot_GodClaim(t *testing.T) {
	t.Parallel()

	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{Subject: "admin"},
		Claims:      map[string]any{"god": true},
	}
	ctx := WithClaims(context.Background(), claims)

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.True(t, got.GetGod())
	// God still carries an identity — auditors need to know *who* the
	// godly caller was.
	require.Equal(t, "admin", got.GetIdentity().GetSubject())
}

func TestResolveCallerSnapshot_ForwardedShortCircuitsClaims(t *testing.T) {
	t.Parallel()

	// Local claims describe the peer (cluster-secret would normally yield no
	// claims, but we set them here to verify the forwarded slot takes priority
	// even when both are present).
	claims := &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{Subject: "peer-node"},
	}
	forwarded := &commonpb.CallerSnapshot{
		Identity: &commonpb.CallerIdentity{
			Subject: "original-user",
			Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
		},
		Scopes: []string{"transactions:write"},
	}

	ctx := WithClaims(context.Background(), claims)
	ctx = WithForwardedSnapshot(ctx, forwarded)

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, "original-user", got.GetIdentity().GetSubject(),
		"forwarded snapshot must win over local peer claims")
	require.Equal(t, "https://idp.example.com", got.GetIdentity().GetIssuer())
	require.Equal(t, []string{"transactions:write"}, got.GetScopes())
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

func TestForwardedSnapshotFromContext_DefaultsNil(t *testing.T) {
	t.Parallel()

	require.Nil(t, ForwardedSnapshotFromContext(context.Background()))
}

func TestWithForwardedSnapshot_RoundTrip(t *testing.T) {
	t.Parallel()

	snapshot := &commonpb.CallerSnapshot{
		Identity: &commonpb.CallerIdentity{Subject: "abc"},
	}
	ctx := WithForwardedSnapshot(context.Background(), snapshot)

	got := ForwardedSnapshotFromContext(ctx)
	require.Same(t, snapshot, got)
}

// CallerIdentity must be free of authorization data. This test fails if
// anyone re-adds scopes or god to the identity proto, which would
// re-introduce the conceptual mix we just split apart.
func TestCallerIdentity_DoesNotCarryAuthorizationFields(t *testing.T) {
	t.Parallel()

	id := &commonpb.CallerIdentity{}
	desc := id.ProtoReflect().Descriptor()

	for i := range desc.Fields().Len() {
		name := string(desc.Fields().Get(i).Name())
		assert.NotEqual(t, "scopes", name, "CallerIdentity must not carry scopes (belongs to CallerSnapshot)")
		assert.NotEqual(t, "god", name, "CallerIdentity must not carry god (belongs to CallerSnapshot)")
	}
}
