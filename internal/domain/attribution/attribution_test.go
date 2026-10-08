package attribution

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestNewRejectsInvalidAttribution(t *testing.T) {
	t.Parallel()

	tests := map[string]*ledgerpb.CallerSnapshot{
		"missing": nil,
		"zero":    {},
		"empty credential source": {Principal: &ledgerpb.CallerSnapshot_Authenticated{
			Authenticated: &ledgerpb.AuthenticatedCaller{Identity: &ledgerpb.CallerIdentity{}},
		}},
		"unsorted scopes": {Principal: &ledgerpb.CallerSnapshot_Anonymous{
			Anonymous: &ledgerpb.AnonymousCaller{Scopes: []string{"z", "a"}},
		}},
		"unknown system": {Principal: &ledgerpb.CallerSnapshot_System{
			System: &ledgerpb.SystemCaller{Component: "invented"},
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

	original := &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_Authenticated{
		Authenticated: &ledgerpb.AuthenticatedCaller{
			Identity: &ledgerpb.CallerIdentity{Source: &ledgerpb.CallerIdentity_KeyId{KeyId: "key-1"}},
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

func TestAllowlistedSystemPrincipal(t *testing.T) {
	t.Parallel()

	_, err := New(&ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_System{
		System: &ledgerpb.SystemCaller{Component: string(ComponentMirror)},
	}})
	require.NoError(t, err)
}

func TestClusterPeerIsAllowlistedSystemPrincipal(t *testing.T) {
	t.Parallel()

	_, err := NewSystem(ComponentClusterPeer)
	require.NoError(t, err)
}
