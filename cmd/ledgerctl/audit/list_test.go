package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"testing"

	"github.com/pterm/pterm"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

type pagedAuditServer struct {
	servicepb.UnimplementedBucketServiceServer

	cursors []string
}

func (s *pagedAuditServer) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream grpc.ServerStreamingServer[auditpb.AuditEntry]) error {
	cursor := req.GetOptions().GetCursor()
	s.cursors = append(s.cursors, cursor)
	if cursor == "" {
		if err := stream.Send(&auditpb.AuditEntry{Sequence: 1}); err != nil {
			return err
		}
		stream.SetTrailer(metadata.Pairs(cmdutil.NextCursorTrailerKey, "page-2"))

		return nil
	}

	return stream.Send(&auditpb.AuditEntry{Sequence: 2})
}

func (s *pagedAuditServer) GetAuditEntry(_ context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	return &auditpb.AuditEntry{Sequence: req.GetSequence()}, nil
}

func TestAuditListAllDecodedOrders(t *testing.T) {
	// Sequential because structured output writes to process-wide os.Stdout.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	backend := &pagedAuditServer{}
	servicepb.RegisterBucketServiceServer(server, backend)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	cmd := NewListCommand()
	cmd.Flags().String("server", listener.Addr().String(), "")
	cmd.Flags().Bool("insecure", true, "")
	cmd.Flags().String("auth-token", "", "")
	cmd.SetContext(t.Context())
	for _, flag := range []string{"all", "expand", "decode-orders", "json"} {
		require.NoError(t, cmd.Flags().Set(flag, "true"))
	}
	read, write, err := os.Pipe()
	require.NoError(t, err)
	previous := os.Stdout
	os.Stdout = write
	err = runList(cmd, nil)
	os.Stdout = previous
	require.NoError(t, write.Close())
	require.NoError(t, err)
	output, err := io.ReadAll(read)
	require.NoError(t, err)
	require.NoError(t, read.Close())
	var decoded []decodedAuditEntry
	require.NoError(t, json.Unmarshal(output, &decoded))
	require.Len(t, decoded, 2)
	require.Equal(t, []string{"", "page-2"}, backend.cursors)
	require.NoError(t, cmd.Flags().Set("expand", "false"))
	require.ErrorContains(t, runList(cmd, nil), "--decode-orders requires --expand")
	require.NoError(t, cmd.Flags().Set("expand", "true"))
	require.NoError(t, cmd.Flags().Set("json", "false"))
	require.ErrorContains(t, runList(cmd, nil), "--decode-orders requires --expand and --json or --yaml")
}

func TestDecodeAuditEntries(t *testing.T) {
	t.Parallel()
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "L", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}}}}}
	serialized, err := order.MarshalVT()
	require.NoError(t, err)
	indexOrder := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "L", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{CreateIndex: &raftcmdpb.CreateIndexOrder{Id: &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE}}}}}}}}}
	indexBytes, err := indexOrder.MarshalVT()
	require.NoError(t, err)
	entry := &auditpb.AuditEntry{Sequence: 5, Timestamp: &commonpb.Timestamp{Data: 1234}, Idempotency: &commonpb.Idempotency{Key: "attempt"}, OrderCount: 2, Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 8, MaxLogSequence: 9}}, Items: []*auditpb.AuditItem{{SerializedOrder: serialized, LogSequence: 8}, {OrderIndex: 1, SerializedOrder: indexBytes, LogSequence: 9}}}
	decoded, err := decodeAuditEntries([]*auditpb.AuditEntry{entry})
	require.NoError(t, err)
	body, err := json.Marshal(decoded)
	require.NoError(t, err)
	require.Contains(t, string(body), `"timestamp":{"data":"1234"}`)
	require.Contains(t, string(body), `"minLogSequence":"8"`)
	require.Contains(t, string(body), `"logSequence":"8"`)
	require.NotContains(t, string(body), `"orderIndex":0`)
	require.Contains(t, string(body), `"ledgerScoped":{"ledger":"L","createLedger":{}}`)
	require.Contains(t, string(body), `"createIndex":{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}`)
}

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
