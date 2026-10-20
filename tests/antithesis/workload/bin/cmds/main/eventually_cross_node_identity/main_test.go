package main

import (
	"context"
	"math"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func TestWaitNodeAtIndexValidatesTarget(t *testing.T) {
	t.Parallel()
	const highID = uint64(math.MaxUint32) + 2
	const targetIndex = uint64(42)
	for _, tc := range []struct {
		name      string
		targetID  uint64
		localID   uint64
		fullWidth bool
		wantReady bool
	}{
		{name: "legacy leader response cannot qualify high target", targetID: highID, localID: 1},
		{name: "wrong full-width node cannot qualify high target", targetID: highID, localID: 1, fullWidth: true},
		{name: "exact full-width node qualifies", targetID: highID, localID: highID, fullWidth: true, wantReady: true},
		{name: "legacy small node qualifies", targetID: 1, localID: 1, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &clusterpb.ClusterState{
				LocalNode: clusterpb.LegacyNodeID(tc.localID),
				RaftStatus: &clusterpb.RaftStatus{
					LastPersistedIndex: targetIndex,
				},
				SyncProgress: &clusterpb.SyncProgress{Status: "normal"},
				Nodes: []*clusterpb.NodeInfo{{
					Id:       clusterpb.LegacyNodeID(tc.localID),
					Suffrage: "Learner",
				}},
			}
			if tc.fullWidth {
				state.LocalNode64 = new(tc.localID)
				state.Nodes[0].Id64 = tc.localID
			}

			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer()
			clusterpb.RegisterClusterServiceServer(server, &identityStatusServer{state: state})
			serveDone := make(chan error, 1)
			go func() { serveDone <- server.Serve(listener) }()
			t.Cleanup(func() {
				server.Stop()
				require.NoError(t, <-serveDone)
			})
			conn, err := grpc.NewClient("passthrough:///identity-status",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			c := &internal.PerNodeConn{NodeID: tc.targetID, Cluster: clusterpb.NewClusterServiceClient(conn)}

			ready, ok := waitNodeAtIndex(t.Context(), c, targetIndex)
			require.Equal(t, tc.wantReady, ok)
			if tc.wantReady {
				require.Same(t, c, ready.conn)
				require.True(t, ready.isLearner)
			} else {
				require.Nil(t, ready.conn, "another node's cursor must not qualify the target")
			}
		})
	}
}

type identityStatusServer struct {
	clusterpb.UnimplementedClusterServiceServer

	state *clusterpb.ClusterState
}

func (s *identityStatusServer) GetClusterState(context.Context, *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	return s.state, nil
}
