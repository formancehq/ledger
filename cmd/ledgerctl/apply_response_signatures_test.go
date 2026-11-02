package main

import (
	"context"
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/numscripts"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/querycheckpoint"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/transactions"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Exercise each migrated command through its real Apply RPC: verification must
// happen before reporting success or consuming the created checkpoint ID.
func TestQueryMutationResponseSignatures(t *testing.T) {
	t.Parallel()

	commands := []struct {
		name string
		args []string
	}{
		{"create query", []string{"queries", "create", "active", "--ledger", "test"}},
		{"delete query", []string{"queries", "delete", "active", "--ledger", "test"}},
		{"update query", []string{"queries", "update", "active", "--ledger", "test", "--filter", "metadata[active] == true"}},
		{"create checkpoint", []string{"query-checkpoint", "create", "--json"}},
		{"delete checkpoint", []string{"query-checkpoint", "delete", "42", "--json"}},
	}
	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			for _, signature := range []struct {
				name      string
				verify    bool
				signed    bool
				corrupt   bool
				alterLog  bool
				wantError string
			}{
				{name: "missing", verify: true, wantError: "log 7: missing response signature"},
				{name: "invalid", verify: true, signed: true, corrupt: true, wantError: "log 7: response signature verification failed"},
				{name: "altered visible log", verify: true, signed: true, alterLog: true, wantError: "log 8: visible response log does not match signed payload"},
				{name: "valid", verify: true, signed: true},
				{name: "verification disabled"},
			} {
				t.Run(signature.name, func(t *testing.T) {
					signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
					log := &commonpb.Log{
						Sequence: 7,
						Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{
							CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 42, MaxSequence: 7},
						}},
					}
					if signature.signed {
						log.ResponseSignature = signer.SignLog(log)
						require.NotNil(t, log.GetResponseSignature())
						if signature.corrupt {
							log.ResponseSignature.Signature[0] ^= 1
						}
						if signature.alterLog {
							log.Sequence = 8
						}
					}

					listener, err := net.Listen("tcp4", "127.0.0.1:0")
					require.NoError(t, err)
					server := grpc.NewServer()
					fixture := &queryMutationSignatureServer{response: &servicepb.ApplyResponse{Logs: []*commonpb.Log{log}}}
					servicepb.RegisterBucketServiceServer(server, fixture)
					go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
					t.Cleanup(server.Stop)

					cmd := &cobra.Command{Use: "ledgerctl", SilenceUsage: true, SilenceErrors: true}
					cmd.PersistentFlags().String("server", listener.Addr().String(), "")
					cmd.PersistentFlags().Bool("insecure", true, "")
					cmd.PersistentFlags().String("auth-token", "test-token", "")
					keyPath := ""
					if signature.verify {
						keyPath = filepath.Join(t.TempDir(), "response.pub")
						require.NoError(t, os.WriteFile(keyPath, signer.PublicKey(), 0o600))
					}
					cmd.PersistentFlags().String("response-verify-key", keyPath, "")
					cmd.AddCommand(queries.NewCommand(), querycheckpoint.NewCommand())
					cmd.SetArgs(command.args)
					err = cmd.Execute()
					if signature.wantError != "" {
						require.ErrorContains(t, err, signature.wantError)
					} else {
						require.NoError(t, err)
					}
					require.EqualValues(t, 1, fixture.calls.Load(), "the command must reach Apply")
				})
			}
		})
	}
}

type queryMutationSignatureServer struct {
	servicepb.UnimplementedBucketServiceServer

	response *servicepb.ApplyResponse
	calls    atomic.Int32
	request  *servicepb.ApplyRequest
}

func (s *queryMutationSignatureServer) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.calls.Add(1)
	s.request = request

	return s.response, nil
}

func TestQueryUpdateWithoutFilterClearsIt(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	fixture := &queryMutationSignatureServer{response: &servicepb.ApplyResponse{}}
	servicepb.RegisterBucketServiceServer(server, fixture)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	cmd := &cobra.Command{Use: "ledgerctl", SilenceUsage: true, SilenceErrors: true}
	cmd.PersistentFlags().String("server", listener.Addr().String(), "")
	cmd.PersistentFlags().Bool("insecure", true, "")
	cmd.PersistentFlags().String("auth-token", "test-token", "")
	cmd.PersistentFlags().String("response-verify-key", "", "")
	cmd.AddCommand(queries.NewCommand())
	cmd.SetArgs([]string{"queries", "update", "all-accounts", "--ledger", "test"})

	require.NoError(t, cmd.Execute())
	require.EqualValues(t, 1, fixture.calls.Load())
	require.NotNil(t, fixture.request)
	requests := fixture.request.GetUnsigned().GetRequests()
	require.Len(t, requests, 1)
	update := requests[0].GetUpdatePreparedQuery()
	require.NotNil(t, update)
	require.Equal(t, "test", update.GetLedger())
	require.Equal(t, "all-accounts", update.GetName())
	require.Nil(t, update.GetFilter(), "omitting --filter must send nil to clear the stored filter")
}

// This test runs serially because pterm output is process-global.
func TestMetadataAndNumscriptMutationResponseSignatures(t *testing.T) {
	scriptPath := filepath.Join(t.TempDir(), "transfer.num")
	require.NoError(t, os.WriteFile(scriptPath, []byte("send [USD 100] (source = @world destination = @bank)"), 0o600))
	commands := []struct {
		name    string
		args    []string
		success string
	}{
		{"set metadata", []string{"transactions", "set-metadata", "42", "--ledger", "test", "--metadata", "status=processed"}, "Metadata set"},
		{"delete metadata", []string{"transactions", "delete-metadata", "42", "status", "--ledger", "test", "--yes"}, "Deleted"},
		{"save numscript", []string{"numscripts", "save", "transfer", "--ledger", "test", "--version", "1.0.0", "--file", scriptPath}, "Saved"},
	}
	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			for _, signature := range []struct {
				name                                                    string
				verify, signed, corrupt, wrongKey, alterLog, invalidKey bool
				wantError                                               string
			}{
				{name: "missing", verify: true, wantError: "log 7: missing response signature"},
				{name: "corrupt", verify: true, signed: true, corrupt: true, wantError: "log 7: response signature verification failed"},
				{name: "wrong key", verify: true, signed: true, wrongKey: true, wantError: "log 7: response signature verification failed"},
				{name: "altered visible log", verify: true, signed: true, alterLog: true, wantError: "log 8: visible response log does not match signed payload"},
				{name: "invalid key file", verify: true, invalidKey: true, wantError: "failed to load response verify key"},
				{name: "valid", verify: true, signed: true},
				{name: "verification disabled"},
			} {
				t.Run(signature.name, func(t *testing.T) {
					seed := make([]byte, ed25519.SeedSize)
					trusted := signing.NewResponseSigner(seed)
					if signature.wrongKey {
						seed[0] = 1
					}
					signer := signing.NewResponseSigner(seed)
					log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_SavedNumscript{SavedNumscript: &commonpb.SavedNumscriptLog{
							Info: &commonpb.NumscriptInfo{Name: "returned-script", Version: "9.8.7"},
						}},
					}}
					if signature.signed {
						log.ResponseSignature = signer.SignLog(log)
						require.NotNil(t, log.GetResponseSignature())
						if signature.corrupt {
							log.ResponseSignature.Signature[0] ^= 1
						}
						if signature.alterLog {
							log.Sequence = 8
						}
					}
					listener, err := net.Listen("tcp4", "127.0.0.1:0")
					require.NoError(t, err)
					server := grpc.NewServer()
					fixture := &queryMutationSignatureServer{response: &servicepb.ApplyResponse{Logs: []*commonpb.Log{log}}}
					servicepb.RegisterBucketServiceServer(server, fixture)
					go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
					t.Cleanup(server.Stop)

					keyPath := ""
					if signature.verify {
						keyPath = filepath.Join(t.TempDir(), "response.pub")
						key := trusted.PublicKey()
						if signature.invalidKey {
							key = []byte("invalid key")
						}
						require.NoError(t, os.WriteFile(keyPath, key, 0o600))
					}
					cmd := &cobra.Command{Use: "ledgerctl", SilenceUsage: true, SilenceErrors: true}
					cmd.PersistentFlags().String("server", listener.Addr().String(), "")
					cmd.PersistentFlags().Bool("insecure", true, "")
					cmd.PersistentFlags().String("auth-token", "test-token", "")
					cmd.PersistentFlags().String("response-verify-key", keyPath, "")
					cmd.AddCommand(transactions.NewCommand(), numscripts.NewCommand())
					cmd.SetArgs(command.args)

					output, err := os.Create(filepath.Join(t.TempDir(), "output"))
					require.NoError(t, err)
					originalSpinnerWriter := pterm.DefaultSpinner.Writer
					pterm.SetDefaultOutput(output)
					pterm.DefaultSpinner.Writer = output
					t.Cleanup(func() {
						pterm.SetDefaultOutput(os.Stdout)
						pterm.DefaultSpinner.Writer = originalSpinnerWriter
						require.NoError(t, output.Close())
					})
					err = cmd.ExecuteContext(t.Context())
					require.EqualValues(t, 1, fixture.calls.Load(), "the command must reach Apply")
					printed, readErr := os.ReadFile(output.Name())
					require.NoError(t, readErr)
					if signature.wantError != "" {
						require.ErrorContains(t, err, signature.wantError)
						require.NotContains(t, string(printed), command.success)
						require.NotContains(t, string(printed), "returned-script")
						require.NotContains(t, string(printed), "9.8.7")
					} else {
						require.NoError(t, err)
						require.Contains(t, string(printed), command.success)
						if command.name == "save numscript" {
							require.Contains(t, string(printed), "returned-script")
							require.Contains(t, string(printed), "9.8.7")
						}
					}
				})
			}
		})
	}
}
