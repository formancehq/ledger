package cluster

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type transferLeaderTestServer struct {
	clusterpb.UnimplementedClusterServiceServer

	targets chan uint64
}

func (s *transferLeaderTestServer) TransferLeadership(_ context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
	s.targets <- req.GetTransferee()

	return &clusterpb.TransferLeadershipResponse{NewLeader: req.GetTransferee()}, nil
}

func newTransferLeaderTestCommand(t *testing.T) (*cobra.Command, <-chan uint64) {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	service := &transferLeaderTestServer{targets: make(chan uint64, 1)}
	clusterpb.RegisterClusterServiceServer(server, service)
	go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
	t.Cleanup(server.Stop)

	cmd := NewTransferLeaderCommand()
	cmd.Flags().String("server", listener.Addr().String(), "")
	cmd.Flags().Bool("insecure", true, "")
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	return cmd, service.targets
}

func TestTransferLeaderCommandPreservesFullNodeID(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		argument string
		want     uint64
	}{
		{argument: "1", want: 1},
		{argument: "4294967297", want: 4294967297},
		{argument: "18446744073709551615", want: 18446744073709551615},
	} {
		t.Run(tc.argument, func(t *testing.T) {
			// Pterm's shared success printer mutates its prefix while printing.
			cmd, targets := newTransferLeaderTestCommand(t)
			cmd.SetArgs([]string{tc.argument})

			require.NoError(t, cmd.Execute())
			require.Equal(t, tc.want, <-targets)
		})
	}
}

func TestTransferLeaderCommandRejectsInvalidNodeIDBeforeRPC(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		argument string
		wantErr  string
	}{
		{argument: "0", wantErr: "node ID must be non-zero"},
		{argument: "not-a-node", wantErr: "invalid node ID"},
		{argument: "-1", wantErr: "invalid node ID"},
		{argument: "18446744073709551616", wantErr: "value out of range"},
	} {
		t.Run(tc.argument, func(t *testing.T) {
			t.Parallel()

			cmd, targets := newTransferLeaderTestCommand(t)
			cmd.SetArgs([]string{"--", tc.argument})

			require.ErrorContains(t, cmd.Execute(), tc.wantErr)
			require.Empty(t, targets, "invalid input must not reach the cluster service")
		})
	}
}
