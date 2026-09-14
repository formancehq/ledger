package main

import (
	"context"
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/queries"
	"github.com/formancehq/ledger/v3/cmd/ledgerctl/querycheckpoint"
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
				wantError string
			}{
				{name: "missing", verify: true, wantError: "log 7: missing response signature"},
				{name: "invalid", verify: true, signed: true, corrupt: true, wantError: "log 7: response signature verification failed"},
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
}

func (s *queryMutationSignatureServer) Apply(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.calls.Add(1)

	return s.response, nil
}
