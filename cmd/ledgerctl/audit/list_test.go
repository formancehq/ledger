package audit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestCallerLabel_PrincipalKinds(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		snapshot *commonpb.CallerSnapshot
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
			snapshot: &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_Anonymous{Anonymous: &commonpb.AnonymousCaller{}}},
			want:     "anonymous",
		},
		"system": {
			snapshot: &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_System{System: &commonpb.SystemCaller{Component: "mirror"}}},
			want:     "system:mirror",
		},
		"auth disabled": {
			snapshot: &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_AuthDisabled{AuthDisabled: &commonpb.AuthDisabledCaller{}}},
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

func authenticatedCaller(subject, issuer, keyID string) *commonpb.CallerSnapshot {
	identity := &commonpb.CallerIdentity{Subject: subject}
	if keyID != "" {
		identity.Source = &commonpb.CallerIdentity_KeyId{KeyId: keyID}
	} else if issuer != "" {
		identity.Source = &commonpb.CallerIdentity_Issuer{Issuer: issuer}
	}

	return &commonpb.CallerSnapshot{Principal: &commonpb.CallerSnapshot_Authenticated{
		Authenticated: &commonpb.AuthenticatedCaller{Identity: identity},
	}}
}
