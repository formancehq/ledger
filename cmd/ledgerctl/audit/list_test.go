package audit

import (
	"bytes"
	"os"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
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

func TestPrintAuditEntry_BatchSignature(t *testing.T) {
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  "l1",
		Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
	}}}
	serialized, err := order.MarshalVT()
	require.NoError(t, err)

	tests := map[string]struct {
		entry *auditpb.AuditEntry
		want  []string
	}{
		"signed": {
			entry: &auditpb.AuditEntry{Signature: &signaturepb.SignedApplyBatch{KeyId: "admin"}},
			want:  []string{"signature=admin"},
		},
		"unsigned": {
			entry: &auditpb.AuditEntry{},
			want:  []string{"unsigned"},
		},
		"idempotency key": {
			entry: &auditpb.AuditEntry{Idempotency: &commonpb.Idempotency{Key: "ik-1"}},
			want:  []string{"unsigned", "idempotency=ik-1"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			test.entry.Items = []*auditpb.AuditItem{{SerializedOrder: serialized}}

			out := captureOutput(func() { printAuditEntry(test.entry, true) })

			for _, want := range test.want {
				require.Contains(t, out, want)
			}
			require.NotContains(t, out, "key=unsigned")
		})
	}
}

func TestSanitizeDisplayText(t *testing.T) {
	t.Parallel()

	require.Equal(t, "ik-1", sanitizeDisplayText("ik-1"))
	require.Equal(t, "a.b.[2K.c", sanitizeDisplayText("a\rb\x1b[2K\x7fc"))
}

func TestPrintAuditEntry_CompactSignature(t *testing.T) {
	tests := map[string]struct {
		entry *auditpb.AuditEntry
		want  string
	}{
		"signed":   {entry: &auditpb.AuditEntry{Signature: &signaturepb.SignedApplyBatch{KeyId: "admin"}}, want: "signature=admin"},
		"unsigned": {entry: &auditpb.AuditEntry{}, want: "unsigned"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			out := captureOutput(func() { printAuditEntry(test.entry, false) })

			require.Contains(t, out, test.want+"\n")
		})
	}
}

// captureOutput returns what fn prints through pterm, unstyled. pterm has no
// getter for its writer, so it is restored to its default, os.Stdout.
func captureOutput(fn func()) string {
	var out bytes.Buffer
	wasRaw := pterm.RawOutput

	pterm.SetDefaultOutput(&out)
	pterm.DisableStyling()

	defer func() {
		pterm.SetDefaultOutput(os.Stdout)
		if !wasRaw {
			pterm.EnableStyling()
		}
	}()

	fn()

	return out.String()
}
