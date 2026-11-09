package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type numscriptExecuteServer struct {
	servicepb.UnimplementedBucketServiceServer

	result   *commonpb.LedgerLogPayload
	requests chan *servicepb.ApplyRequest
}

func (s *numscriptExecuteServer) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.requests <- request

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{Data: s.result},
	}}}}}}, nil
}

func TestNumscriptExecuteBothResults(t *testing.T) {
	configDir := t.TempDir()
	t.Setenv("HOME", configDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("LEDGERCTL_PROFILE", "")
	file := filepath.Join(t.TempDir(), "script.num")
	require.NoError(t, os.WriteFile(file, []byte("set_account_meta(@users:alice, \"tier\", \"gold\")"), 0o600))

	created := &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 7}}
	updated := &commonpb.AccountMetadataUpdated{AccountMetadata: map[string]*commonpb.MetadataMap{
		"users:alice": {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("gold")}},
	}}
	for _, tc := range []struct {
		name   string
		result *commonpb.LedgerLogPayload
		saved  bool
	}{
		{name: "transaction", result: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created}}},
		{name: "saved-transaction", result: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created}}, saved: true},
		{name: "metadata", result: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AccountMetadataUpdated{AccountMetadataUpdated: updated}}},
		{name: "saved-metadata", result: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AccountMetadataUpdated{AccountMetadataUpdated: updated}}, saved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &numscriptExecuteServer{result: tc.result, requests: make(chan *servicepb.ApplyRequest, 1)}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, fixture)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			root := newRootCommand()
			args := []string{"numscripts", "execute", "--ledger", "fresh", "--file", file,
				"--server", listener.Addr().String(), "--insecure", "--json", "--tls-ca-cert=", "--tls-server-name=", "--result-file=", "--signing-key=", "--response-verify-key=", "--auth-token=test-token"}
			if tc.saved {
				args[4], args[5] = "--name", "transfer"
			}
			if tc.name == "transaction" || tc.name == "saved-transaction" {
				args = append(args, "--metadata", "receipt=invoice-42")
			}
			root.SetArgs(args)
			require.NoError(t, root.ExecuteContext(t.Context()))
			request := <-fixture.requests
			require.Equal(t, "fresh", request.GetUnsigned().GetRequests()[0].GetApply().GetLedger())
			payload := request.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction()
			if tc.name == "transaction" || tc.name == "saved-transaction" {
				require.Equal(t, "invoice-42", payload.GetMetadata()["receipt"].GetStringValue())
			}
			if tc.saved {
				require.Equal(t, "transfer", payload.GetScriptReference().GetName())
				require.Equal(t, "latest", payload.GetScriptReference().GetVersion())
			} else {
				require.NotNil(t, payload.GetScript())
			}
		})
	}
}

func TestTransactionsCreateHasNoScriptFlag(t *testing.T) {
	root := newRootCommand()
	command, _, err := root.Find([]string{"transactions", "create"})
	require.NoError(t, err)
	require.Nil(t, command.Flags().Lookup("script"))
	require.Nil(t, command.Flags().Lookup("var"))
}
