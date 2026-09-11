package bootstrap

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/peer"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
)

// Service forwarding is under test; Raft message transport has no work here.
type serviceAddressRaftTransport struct{}

func (serviceAddressRaftTransport) AddPeer(uint64, string)             {}
func (serviceAddressRaftTransport) RemovePeer(context.Context, uint64) {}

func TestServiceAdvertiseAddr_IPv6RemoteRPC(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp6", "[::1]:0")
	require.NoError(t, err, "the IPv6 regression requires IPv6 loopback")
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health.NewServer())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop() // Also closes the owned listener and active connections.
		select {
		case err := <-serveDone:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("gRPC server did not stop")
		}
	})

	// Hold a distinct Raft socket so replacing its port cannot accidentally
	// pass by dialing the original advertise address.
	raftListener, err := net.Listen("tcp6", "[::1]:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, raftListener.Close()) })
	cfg := Config{
		RaftConfig: node.NodeConfig{
			NodeID:        2,
			AdvertiseAddr: raftListener.Addr().String(),
			WalDir:        t.TempDir(),
		},
		DataDir:  t.TempDir(),
		GRPCPort: listener.Addr().(*net.TCPAddr).Port,
	}
	// Exercise the same provider registered in the bootstrap Fx graph.
	published, err := buildNodeConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, cfg.RaftConfig.AdvertiseAddr, published.AdvertiseAddr)
	require.Equal(t, cfg.DataDir, published.DataDir)
	require.Len(t, published.InstanceID, 16)

	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	peerStore := membership.NewPeerStore(newTestStore(t))
	remote, err := membership.NewMembership(peerStore, serviceAddressRaftTransport{}, pool,
		1, "", "", nil, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, remote.Register(published.NodeID, published.AdvertiseAddr,
		published.ServiceAdvertiseAddr, published.InstanceID))
	remote.Start()
	conn := pool.GetConnection(published.NodeID)
	require.NotNil(t, conn)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var reached peer.Peer
	response, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{},
		grpc.WaitForReady(true), grpc.Peer(&reached))
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, response.GetStatus())
	require.Equal(t, listener.Addr().String(), reached.Addr.String())
	require.Equal(t, listener.Addr().String(), pool.GetPeerAddress(published.NodeID))
	stored, err := peerStore.LoadAll()
	require.NoError(t, err)
	require.Equal(t, listener.Addr().String(), stored[published.NodeID].ServiceAddress)
}
