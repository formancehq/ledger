package store

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type metricsIdentityServer struct {
	servicepb.UnimplementedBucketServiceServer

	primary   chan uint64
	secondary chan uint64
}

func (s *metricsIdentityServer) GetPrimaryMetrics(_ context.Context, req *servicepb.GetPrimaryMetricsRequest) (*servicepb.GetPrimaryMetricsResponse, error) {
	s.primary <- req.GetNodeId()

	return &servicepb.GetPrimaryMetricsResponse{Available: true, Metrics: &servicepb.PebbleMetrics{}}, nil
}

func (s *metricsIdentityServer) GetSecondaryMetrics(_ context.Context, req *servicepb.GetSecondaryMetricsRequest) (*servicepb.GetSecondaryMetricsResponse, error) {
	s.secondary <- req.GetNodeId()

	return &servicepb.GetSecondaryMetricsResponse{Available: true, Metrics: &servicepb.PebbleMetrics{}}, nil
}

func TestMetricsCommandsPreserveFullNodeID(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	service := &metricsIdentityServer{primary: make(chan uint64, 1), secondary: make(chan uint64, 1)}
	servicepb.RegisterBucketServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	for _, tc := range []struct {
		name       string
		newCommand func() *cobra.Command
		requests   <-chan uint64
	}{
		{"primary", NewPrimaryMetricsCommand, service.primary},
		{"secondary", NewSecondaryMetricsCommand, service.secondary},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Structured output avoids Pterm's shared formatted-metrics printers.
			for _, id := range []uint64{0, 1, 1<<32 + 1, ^uint64(0)} {
				t.Run(strconv.FormatUint(id, 10), func(t *testing.T) {
					cmd := tc.newCommand()
					cmd.Flags().String("server", listener.Addr().String(), "")
					cmd.Flags().Bool("insecure", true, "")
					cmd.SetArgs([]string{"--json", "--node-id", strconv.FormatUint(id, 10)})
					require.NoError(t, cmd.Execute())
					require.Len(t, tc.requests, 1)
					require.Equal(t, id, <-tc.requests)
				})
			}
			for _, argument := range []string{"-1", "not-a-node", "18446744073709551616"} {
				t.Run(argument, func(t *testing.T) {
					cmd := tc.newCommand()
					cmd.SetOut(io.Discard)
					cmd.SetErr(io.Discard)
					cmd.SilenceUsage = true
					cmd.SilenceErrors = true
					cmd.SetArgs([]string{"--node-id", argument})
					require.ErrorContains(t, cmd.Execute(), "invalid argument")
					require.Empty(t, tc.requests, "invalid input must reject before dialing")
				})
			}
		})
	}
}
