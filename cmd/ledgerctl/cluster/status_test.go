package cluster

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type statusIdentityTestServer struct {
	clusterpb.UnimplementedClusterServiceServer

	targets chan uint64
}

func (s *statusIdentityTestServer) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	s.targets <- req.GetNodeId()

	return &clusterpb.ClusterState{
		Leader: req.GetNodeId(), LocalNode: req.GetNodeId(),
		Nodes: []*clusterpb.NodeInfo{{Id: req.GetNodeId()}},
	}, nil
}

func TestStatusCommandPreservesFullNodeIDAndJSONStrings(t *testing.T) {
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
			t.Parallel()

			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			service := &statusIdentityTestServer{targets: make(chan uint64, 1)}
			clusterpb.RegisterClusterServiceServer(server, service)
			go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
			t.Cleanup(server.Stop)

			// The result file contains the same JSON bytes emitted to stdout and
			// lets parallel cases assert output without replacing global stdout.
			resultPath := filepath.Join(t.TempDir(), "status.json")
			require.NoError(t, os.WriteFile(resultPath, nil, 0o600))
			cmd := NewStatusCommand()
			cmd.Flags().String("server", listener.Addr().String(), "")
			cmd.Flags().Bool("insecure", true, "")
			cmd.Flags().String("result-file", "", "")
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"--node-id", tc.argument, "--json", "--result-file", resultPath})

			require.NoError(t, cmd.Execute())
			require.Equal(t, tc.want, <-service.targets)
			payload, err := os.ReadFile(resultPath)
			require.NoError(t, err)
			var output struct {
				Leader    string `json:"leader"`
				LocalNode string `json:"localNode"`
				Nodes     []struct {
					ID string `json:"id"`
				} `json:"nodes"`
			}
			require.NoError(t, json.Unmarshal(payload, &output), "protobuf uint64 IDs must be decimal JSON strings")
			require.Equal(t, tc.argument, output.Leader)
			require.Equal(t, tc.argument, output.LocalNode)
			require.Len(t, output.Nodes, 1)
			require.Equal(t, tc.argument, output.Nodes[0].ID)
		})
	}
}
