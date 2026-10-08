package audit

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestCallerLabel_PrincipalKinds(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshot *ledgerpb.CallerSnapshot
		want     string
	}{
		"authenticated subject": {
			snapshot: authenticatedCaller("alice", "https://idp.example.com", ""),
			want:     "alice",
		},
		"authenticated key fallback": {
			snapshot: authenticatedCaller("", "", "key-7"),
			want:     "key:key-7",
		},
		"anonymous": {
			snapshot: &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_Anonymous{Anonymous: &ledgerpb.AnonymousCaller{}}},
			want:     "anonymous",
		},
		"system": {
			snapshot: &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_System{System: &ledgerpb.SystemCaller{Component: "mirror"}}},
			want:     "system:mirror",
		},
		"auth disabled": {
			snapshot: &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_AuthDisabled{AuthDisabled: &ledgerpb.AuthDisabledCaller{}}},
			want:     "auth-disabled",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, callerLabel(test.snapshot))
		})
	}
}

func authenticatedCaller(subject, issuer, keyID string) *ledgerpb.CallerSnapshot {
	identity := &ledgerpb.CallerIdentity{Subject: subject}
	if keyID != "" {
		identity.Source = &ledgerpb.CallerIdentity_KeyId{KeyId: keyID}
	} else if issuer != "" {
		identity.Source = &ledgerpb.CallerIdentity_Issuer{Issuer: issuer}
	}

	return &ledgerpb.CallerSnapshot{Principal: &ledgerpb.CallerSnapshot_Authenticated{
		Authenticated: &ledgerpb.AuthenticatedCaller{Identity: identity},
	}}
}
