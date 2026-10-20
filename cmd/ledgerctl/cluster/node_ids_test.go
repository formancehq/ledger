package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type clusterIdentityServer struct {
	clusterpb.UnimplementedClusterServiceServer

	requests chan uint64
	cancel   context.CancelFunc
	state    *clusterpb.ClusterState
	legacy   bool
}

func (s *clusterIdentityServer) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	if s.legacy {
		s.requests <- uint64(req.GetNodeId())

		return &clusterpb.ClusterState{LocalNode: req.GetNodeId()}, nil
	}
	s.requests <- req.TargetID()
	if s.cancel != nil {
		s.cancel()
	}

	if s.state != nil {
		return s.state, nil
	}

	return &clusterpb.ClusterState{LocalNode64: new(req.TargetID())}, nil
}

func (s *clusterIdentityServer) TransferLeadership(_ context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
	if s.legacy {
		s.requests <- uint64(req.GetTransferee())
		if req.GetTransferee() == 0 {
			return nil, errors.New("transferee node ID must be non-zero")
		}

		return &clusterpb.TransferLeadershipResponse{NewLeader: req.GetTransferee()}, nil
	}
	s.requests <- req.TargetID()

	return &clusterpb.TransferLeadershipResponse{NewLeader64: req.TargetID()}, nil
}

func clusterIdentityCommand(t *testing.T, cmd *cobra.Command, server *clusterIdentityServer) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	rpcServer := grpc.NewServer()
	clusterpb.RegisterClusterServiceServer(rpcServer, server)
	go func() { _ = rpcServer.Serve(listener) }() // Stop closes the owned listener.
	t.Cleanup(rpcServer.Stop)
	cmd.Flags().String("server", listener.Addr().String(), "")
	cmd.Flags().Bool("insecure", true, "")
	cmd.Flags().String("auth-token", "test-token", "")
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	cmd.SetContext(ctx)
}

func TestStatusPreservesNodeID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{input: "1", want: 1},
		{input: "0", want: 0},
		{input: "4294967296", want: 4294967296},
		{input: "4294967297", want: 4294967297},
		{input: "18446744073709551615", want: 18446744073709551615},
	} {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			server := &clusterIdentityServer{requests: make(chan uint64, 1)}
			cmd := NewStatusCommand()
			clusterIdentityCommand(t, cmd, server)
			cmd.SetArgs([]string{"--node-id", tc.input, "--json"})
			require.NoError(t, cmd.Execute())
			require.Equal(t, tc.want, <-server.requests)
		})
	}
}

func TestWatchPreservesNodeID(t *testing.T) {
	// pterm's live terminal area is process-global, so watch cases run serially.
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{input: "1", want: 1},
		{input: "0", want: 0},
		{input: "4294967296", want: 4294967296},
		{input: "4294967297", want: 4294967297},
		{input: "18446744073709551615", want: 18446744073709551615},
	} {
		t.Run(tc.input, func(t *testing.T) {
			server := &clusterIdentityServer{requests: make(chan uint64, 1)}
			cmd := NewWatchCommand()
			clusterIdentityCommand(t, cmd, server)
			ctx, cancel := context.WithCancel(cmd.Context())
			t.Cleanup(cancel)
			cmd.SetContext(ctx)
			server.cancel = cancel
			cmd.SetArgs([]string{"--node-id", tc.input, "--interval", "1h"})
			require.NoError(t, cmd.Execute())
			select {
			case id := <-server.requests:
				require.Equal(t, tc.want, id)
			default:
				t.Fatal("watch did not query the server before cancellation")
			}
		})
	}
}

func TestTransferLeaderPreservesNodeID(t *testing.T) {
	// pterm's success printer mutates process-global state during output.
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{input: "1", want: 1},
		{input: "4294967296", want: 4294967296},
		{input: "4294967297", want: 4294967297},
		{input: "18446744073709551615", want: 18446744073709551615},
	} {
		t.Run(tc.input, func(t *testing.T) {
			server := &clusterIdentityServer{requests: make(chan uint64, 1)}
			cmd := NewTransferLeaderCommand()
			clusterIdentityCommand(t, cmd, server)
			cmd.SetArgs([]string{tc.input})
			require.NoError(t, cmd.Execute())
			require.Equal(t, tc.want, <-server.requests)
		})
	}
}

func TestClusterCommandsRejectInvalidNodeID(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		command   func() *cobra.Command
		args      []string
		wantError string
	}{
		{name: "status overflow", command: NewStatusCommand, args: []string{"--node-id", "18446744073709551616"}, wantError: "value out of range"},
		{name: "watch overflow", command: NewWatchCommand, args: []string{"--node-id", "18446744073709551616"}, wantError: "value out of range"},
		{name: "transfer overflow", command: NewTransferLeaderCommand, args: []string{"18446744073709551616"}, wantError: "value out of range"},
		{name: "transfer zero", command: NewTransferLeaderCommand, args: []string{"0"}, wantError: "node ID must be non-zero"},
		{name: "transfer negative", command: NewTransferLeaderCommand, args: []string{"--", "-1"}, wantError: "invalid node ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := &clusterIdentityServer{requests: make(chan uint64, 1)}
			cmd := tc.command()
			clusterIdentityCommand(t, cmd, server)
			cmd.SetArgs(tc.args)
			require.ErrorContains(t, cmd.Execute(), tc.wantError)
			require.Empty(t, server.requests, "invalid input must not reach the RPC")
		})
	}
}

func TestClusterStatusDisplaysFullNodeIDs(t *testing.T) {
	t.Parallel()
	output := renderClusterStatus(&clusterpb.ClusterState{
		Leader64:    proto.Uint64(4294967297),
		LocalNode64: proto.Uint64(4294967296),
		Nodes: []*clusterpb.NodeInfo{
			{Id: 1, Suffrage: "Voter"},
			{Id64: 4294967296, Suffrage: "Voter"},
			{Id64: 4294967297, Suffrage: "Voter"},
			{Id64: 18446744073709551615, Suffrage: "Learner"},
		},
	}, false)
	require.Regexp(t, `Local Node ID:[^\n]*4294967296`, output)
	require.Regexp(t, `Leader ID:[^\n]*4294967297`, output)
	require.Contains(t, output, "18446744073709551615")
	require.NotContains(t, output, "No leader")
	require.Contains(t, renderClusterStatus(&clusterpb.ClusterState{}, false), "No leader")
}

// Stdout is process-global; this command-output test must run serially.
func TestStatusJSONPreservesDecimalStringNodeIDs(t *testing.T) {
	server := &clusterIdentityServer{requests: make(chan uint64, 1), state: &clusterpb.ClusterState{
		Leader64: proto.Uint64(4294967297), LocalNode64: proto.Uint64(4294967296), Nodes: []*clusterpb.NodeInfo{{Id: 1}, {Id64: 4294967297}, {Id64: 18446744073709551615}},
	}}
	cmd := NewStatusCommand()
	clusterIdentityCommand(t, cmd, server)
	cmd.SetArgs([]string{"--node-id", "4294967296", "--json"})
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	original := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = original; _ = reader.Close(); _ = writer.Close() }) // Owned pipe cleanup after an early assertion failure.
	require.NoError(t, cmd.Execute())
	os.Stdout = original
	require.NoError(t, writer.Close())
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	var output struct {
		Leader    string `json:"leader"`
		LocalNode string `json:"localNode"`
		Nodes     []struct {
			ID json.RawMessage `json:"id"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(data, &output))
	require.Equal(t, "4294967297", output.Leader)
	require.Equal(t, "4294967296", output.LocalNode)
	ids := make([]string, 0, len(output.Nodes))
	for _, info := range output.Nodes {
		ids = append(ids, string(info.ID))
	}
	require.Equal(t, []string{`1`, `"4294967297"`, `"18446744073709551615"`}, ids)
}

func TestClusterCommandsAgainstLegacyServer(t *testing.T) {
	// Transfer command output uses process-global pterm state.
	for _, id := range []uint64{1, 4294967296, 4294967297} {
		for _, command := range []string{"status", "transfer"} {
			t.Run(command+strconv.FormatUint(id, 10), func(t *testing.T) {
				server := &clusterIdentityServer{requests: make(chan uint64, 1), legacy: true}
				var cmd *cobra.Command
				if command == "status" {
					cmd = NewStatusCommand()
					cmd.SetArgs([]string{"--node-id", strconv.FormatUint(id, 10), "--json"})
				} else {
					cmd = NewTransferLeaderCommand()
					cmd.SetArgs([]string{strconv.FormatUint(id, 10)})
				}
				clusterIdentityCommand(t, cmd, server)
				err := cmd.Execute()
				if id == 1 {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				require.Equal(t, uint64(clusterpb.LegacyNodeID(id)), <-server.requests)
			})
		}
	}
}

func TestStatusOutputRemainsReadableByLegacyOperator(t *testing.T) {
	t.Parallel()
	state := &clusterpb.ClusterState{Leader64: proto.Uint64(1), LocalNode64: proto.Uint64(1), Nodes: []*clusterpb.NodeInfo{{Id64: 1}, {Id64: 3}}}
	data, err := json.Marshal(statusOutput{state})
	require.NoError(t, err)
	var legacy struct {
		Leader    uint32 `json:"leader"`
		LocalNode uint32 `json:"localNode"`
		Nodes     []struct {
			ID uint32 `json:"id"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(data, &legacy))
	require.Equal(t, uint32(1), legacy.Leader)
	require.Equal(t, uint32(1), legacy.LocalNode)
	require.Equal(t, uint32(3), legacy.Nodes[1].ID)
}
