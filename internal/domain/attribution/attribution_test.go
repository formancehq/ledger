package attribution

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestNewRejectsInvalidAttribution(t *testing.T) {
	t.Parallel()

	tests := map[string]*commonpb.CallerSnapshot{
		"missing": nil,
		"zero":    {},
		"empty credential source": {Principal: &commonpb.CallerSnapshot_Authenticated{
			Authenticated: &commonpb.AuthenticatedCaller{Identity: &commonpb.CallerIdentity{}},
		}},
		"unsorted scopes": {Principal: &commonpb.CallerSnapshot_Anonymous{
			Anonymous: &commonpb.AnonymousCaller{Scopes: []string{"z", "a"}},
		}},
		"unknown system": {Principal: &commonpb.CallerSnapshot_System{
			System: &commonpb.SystemCaller{Component: "invented"},
		}},
	}

	for name, snapshot := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := New(snapshot)
			var invalid *domain.ErrInvalidCallerAttribution
			require.ErrorAs(t, err, &invalid)
		})
	}
}

func TestCapabilityFreezesAndClonesSnapshot(t *testing.T) {
	t.Parallel()

	original := &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_Authenticated{
		Authenticated: &commonpb.AuthenticatedCaller{
			Identity: &commonpb.CallerIdentity{Source: &commonpb.CallerIdentity_KeyId{KeyId: "key-1"}},
			Scopes:   []string{"ledger:Read", "ledger:Write"},
		},
	}}
	capability, err := New(original)
	require.NoError(t, err)

	original.GetAuthenticated().Scopes[0] = "tampered"
	first := capability.Snapshot()
	require.Equal(t, []string{"ledger:Read", "ledger:Write"}, first.GetAuthenticated().GetScopes())

	first.GetAuthenticated().Scopes[0] = "changed"
	second := capability.Snapshot()
	require.Equal(t, []string{"ledger:Read", "ledger:Write"}, second.GetAuthenticated().GetScopes())
}

func TestHistoricalScopeNamesRemainValidAttribution(t *testing.T) {
	t.Parallel()

	snapshot := &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_Anonymous{
		Anonymous: &commonpb.AnonymousCaller{Scopes: []string{"ledger:TransactionRead", "ledger:TransactionWrite"}},
	}}
	require.NoError(t, Validate(snapshot))
	capability, err := New(snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshot.GetAnonymous().GetScopes(), capability.Snapshot().GetAnonymous().GetScopes())
}

func TestAllowlistedSystemPrincipal(t *testing.T) {
	t.Parallel()

	_, err := New(&commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_System{
		System: &commonpb.SystemCaller{Component: string(ComponentMirror)},
	}})
	require.NoError(t, err)
}

func TestClusterPeerIsAllowlistedSystemPrincipal(t *testing.T) {
	t.Parallel()

	_, err := NewSystem(ComponentClusterPeer)
	require.NoError(t, err)
}
