package bootstrap

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// followerNode is a non-leader that knows node 1 leads.
type followerNode struct{}

func (followerNode) IsLeader() bool    { return false }
func (followerNode) GetLeader() uint64 { return 1 }
func (followerNode) GetNodeID() uint64 { return 2 }

// recyclingLeaderConns hands out one shared connection to the leader, like
// the Raft transport's pool, and can replace it the way
// ConnectionPool.RestartConnection does: dial a new one, close the old one.
type recyclingLeaderConns struct {
	t    *testing.T
	addr string

	mu   sync.Mutex
	conn *grpc.ClientConn
}

func (c *recyclingLeaderConns) dial() *grpc.ClientConn {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(c.t, err)

	return conn
}

func (c *recyclingLeaderConns) GetPeerConnection(uint64) *grpc.ClientConn {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.conn
}

func (c *recyclingLeaderConns) restart() {
	c.mu.Lock()
	old := c.conn
	c.conn = c.dial()
	c.mu.Unlock()

	_ = old.Close()
}

func (c *recyclingLeaderConns) close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = c.conn.Close()
}

// restartingLeader restarts the follower's connection to it while the first
// forwarded JoinAsLearner is in flight, and accepts the second one.
type restartingLeader struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	conns    *recyclingLeaderConns
	attempts atomic.Int32
}

func (l *restartingLeader) JoinAsLearner(ctx context.Context, _ *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	if l.attempts.Add(1) == 1 {
		l.conns.restart()
		<-ctx.Done()

		return nil, status.FromContextError(ctx.Err()).Err()
	}

	return &clusterbootstrappb.JoinAsLearnerResponse{}, nil
}

func serveBootstrap(t *testing.T, impl clusterbootstrappb.ClusterBootstrapServiceServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

// TestTryAddLearner_RetriesWhenFollowerLeaderConnectionRestarts covers a join
// sent to a follower whose shared Raft connection to the leader is restarted
// mid-forward: the forward fails locally with Canceled ("the client connection
// is closing"), and the joiner must retry instead of exiting.
func TestTryAddLearner_RetriesWhenFollowerLeaderConnectionRestarts(t *testing.T) {
	t.Parallel()

	leader := &restartingLeader{}
	leaderAddr := serveBootstrap(t, leader)

	conns := &recyclingLeaderConns{t: t, addr: leaderAddr}
	conns.conn = conns.dial()
	t.Cleanup(conns.close)
	leader.conns = conns

	followerAddr := serveBootstrap(t, grpcadp.NewClusterBootstrapServiceServer(
		followerNode{}, conns, nil, logging.Testing(), "test-cluster",
	))

	walDir := t.TempDir()
	cfg := Config{
		ClusterID: "test-cluster",
		TLSConfig: TLSConfig{Mode: TLSModeDisabled},
		RaftConfig: node.NodeConfig{
			NodeID:        3,
			WalDir:        walDir,
			AdvertiseAddr: "127.0.0.1:7777",
			Peers: []node.Peer{
				{ID: 2, Address: followerAddr, ServiceAddress: "127.0.0.1:8888"},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	require.NoError(t, tryAddLearner(ctx, cfg, cfg.TLSConfig, logging.Testing()))
	require.Equal(t, int32(2), leader.attempts.Load())
	require.True(t, wal.IsClusterJoined(walDir))
}
