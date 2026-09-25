package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger/v3/internal/domain/attribution"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestResolveCallerSnapshot_Unauthenticated(t *testing.T) {
	t.Parallel()

	require.Nil(t, ResolveCallerSnapshot(context.Background()))
}

func TestResolveCallerSnapshot_Anonymous(t *testing.T) {
	t.Parallel()

	ctx := withAuthenticationState(context.Background(), true, false, map[Scope]struct{}{
		ScopeTransactionsWrite: {},
		ScopeTransactionsRead:  {},
	})

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got.GetAnonymous())
	require.Equal(t, []string{string(ScopeTransactionsRead), string(ScopeTransactionsWrite)}, got.GetAnonymous().GetScopes())
}

func TestResolveCallerSnapshot_AuthDisabled(t *testing.T) {
	t.Parallel()

	ctx := withAuthenticationState(context.Background(), false, false, nil)
	require.NotNil(t, ResolveCallerSnapshot(ctx).GetAuthDisabled())
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
	authenticated := got.GetAuthenticated()
	require.NotNil(t, authenticated)
	require.Equal(t, "user-1", authenticated.GetIdentity().GetSubject())
	require.False(t, authenticated.GetGod())
	require.Equal(t, "https://issuer.example.com", authenticated.GetIdentity().GetIssuer())
	// Scopes must be sorted for deterministic Raft serialization.
	require.Equal(t, []string{string(ScopeTransactionsRead), string(ScopeTransactionsWrite)}, authenticated.GetScopes())
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
	require.Equal(t, "ed25519-key-7", got.GetAuthenticated().GetIdentity().GetKeyId())
	require.Empty(t, got.GetAuthenticated().GetIdentity().GetIssuer())
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
	require.True(t, got.GetAuthenticated().GetGod())
	// God still carries an identity — auditors need to know *who* the
	// godly caller was.
	require.Equal(t, "admin", got.GetAuthenticated().GetIdentity().GetSubject())
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
		Principal: &commonpb.CallerSnapshot_Authenticated{
			Authenticated: &commonpb.AuthenticatedCaller{
				Identity: &commonpb.CallerIdentity{
					Subject: "original-user",
					Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
				},
				Scopes: []string{"ledger:TransactionWrite"},
			},
		},
	}

	ctx := WithClaims(context.Background(), claims)
	capability, err := attribution.New(forwarded)
	require.NoError(t, err)
	ctx = WithForwardedAttribution(ctx, capability)

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, "original-user", got.GetAuthenticated().GetIdentity().GetSubject(),
		"forwarded snapshot must win over local peer claims")
	require.Equal(t, "https://idp.example.com", got.GetAuthenticated().GetIdentity().GetIssuer())
	require.Equal(t, []string{"ledger:TransactionWrite"}, got.GetAuthenticated().GetScopes())
}

func TestResolveCallerSnapshot_ClusterInternalWithoutForwardedSnapshot(t *testing.T) {
	t.Parallel()

	ctx := withAuthenticationState(context.Background(), true, false, allScopes())
	ctx = WithClusterInternal(ctx, true)

	require.Nil(t, ResolveCallerSnapshot(ctx),
		"the peer's cluster-secret grant must not be attributed to the original caller")
}

func TestResolveCallerSnapshot_SystemActor(t *testing.T) {
	t.Parallel()

	ctx := WithSystemActor(context.Background(), commands.ComponentQueryCheckpoint)

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, string(commands.ComponentQueryCheckpoint), got.GetSystem().GetComponent())
}

func TestResolveCallerSnapshot_SystemActorWinsOverForwardedAndClaims(t *testing.T) {
	t.Parallel()

	// A system action must be attributed to the system component even if a
	// forwarded snapshot or local claims happen to be present.
	ctx := WithClaims(context.Background(), &oidc.AccessTokenClaims{
		TokenClaims: oidc.TokenClaims{Subject: "user-1"},
	})
	capability, err := attribution.New(&commonpb.CallerSnapshot{
		Principal: &commonpb.CallerSnapshot_Authenticated{
			Authenticated: &commonpb.AuthenticatedCaller{
				Identity: &commonpb.CallerIdentity{
					Subject: "forwarded-user",
					Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
				},
			},
		},
	})
	require.NoError(t, err)
	ctx = WithForwardedAttribution(ctx, capability)
	ctx = WithSystemActor(ctx, commands.ComponentMirror)

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Equal(t, string(commands.ComponentMirror), got.GetSystem().GetComponent())
}

func TestResolveCallerSnapshot_EmptySystemComponentFallsThrough(t *testing.T) {
	t.Parallel()

	// An empty component falls through to the missing-state result.
	ctx := WithSystemActor(context.Background(), attribution.SystemActor(""))

	require.Nil(t, ResolveCallerSnapshot(ctx))
}

func TestResolveCallerSnapshot_Ed25519WithoutSubject(t *testing.T) {
	t.Parallel()

	// An Ed25519 token minted without a `sub` claim still authenticates and
	// must remain attributable: the snapshot is non-nil and the key id
	// identifies the caller even though the subject is empty.
	ctx := WithClaims(context.Background(), &oidc.AccessTokenClaims{})
	ctx = WithKeyID(ctx, "ed25519-key-9")

	got := ResolveCallerSnapshot(ctx)
	require.NotNil(t, got)
	require.Empty(t, got.GetAuthenticated().GetIdentity().GetSubject())
	require.Equal(t, "ed25519-key-9", got.GetAuthenticated().GetIdentity().GetKeyId())
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

func TestWithForwardedAttribution_RoundTripIsIsolated(t *testing.T) {
	t.Parallel()

	snapshot := &commonpb.CallerSnapshot{
		Principal: &commonpb.CallerSnapshot_Authenticated{
			Authenticated: &commonpb.AuthenticatedCaller{
				Identity: &commonpb.CallerIdentity{
					Subject: "abc",
					Source:  &commonpb.CallerIdentity_KeyId{KeyId: "key-1"},
				},
			},
		},
	}
	capability, err := attribution.New(snapshot)
	require.NoError(t, err)
	ctx := WithForwardedAttribution(context.Background(), capability)

	got := ForwardedSnapshotFromContext(ctx)
	require.Equal(t, snapshot, got)
	require.NotSame(t, snapshot, got)
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
