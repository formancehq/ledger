package node

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	transportpkg "github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

func newInitialProbeTransport(t *testing.T) (*DefaultTransport, string) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	pool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}, transportpkg.PoolConfig{})
	tr := NewTransport(logging.Testing(), pool, noop.NewMeterProvider(), 1,
		TransportConfig{Reception: []int{8, 8, 8}, Send: []int{8, 8, 8}}, "test", 1024, "", "")
	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Start(context.Background())
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, tr.Stop(ctx))
		<-done
	})

	// Registration happens while the joining endpoint is closed, as it does
	// during production join preflight. There is no subsequent AddPeer call.
	tr.AddPeer(2, addr)
	require.Equal(t, addr, pool.GetPeerAddress(2))
	tr.peersMu.RLock()
	peer := tr.peers[2]
	tr.peersMu.RUnlock()
	require.NotNil(t, peer, "first probe failure must retain a retry owner")
	select {
	case id := <-tr.Unreachable():
		require.Equal(t, uint64(2), id)
	case <-time.After(5 * time.Second):
		t.Fatal("pending connection did not enter the unreachable/retry path")
	}

	return tr, addr
}

func TestDefaultTransport_InitialProbeFailureRecoversDelivery(t *testing.T) {
	t.Parallel()
	tr, addr := newInitialProbeTransport(t)

	listener, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	server := grpc.NewServer()
	pings := make(chan struct{}, 1)
	messages := make(chan struct{}, 1)
	rafttransportpb.RegisterRaftTransportServiceServer(server, &pingServer{pings: pings, raftMessages: messages})
	go func() { _ = server.Serve(listener) }() // Stop below owns server cleanup.
	t.Cleanup(server.Stop)

	select {
	case <-pings:
	case <-time.After(10 * time.Second):
		t.Fatal("peer did not autonomously reconnect after its listener started")
	}
	tr.Send([]*raftpb.Message{{From: proto.Uint64(1), To: proto.Uint64(2), Type: raftpb.MsgHeartbeat.Enum(), Term: proto.Uint64(1)}})
	select {
	case <-messages:
	case <-time.After(5 * time.Second):
		t.Fatal("Raft message was not delivered after initial probe recovery")
	}
}

func TestDefaultTransport_InitialProbeFailureRemovalStopsRetry(t *testing.T) {
	t.Parallel()
	tr, _ := newInitialProbeTransport(t)
	tr.peersMu.RLock()
	peer := tr.peers[2]
	tr.peersMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tr.RemovePeer(ctx, 2)
	select {
	case <-peer.loopDone:
	default:
		t.Fatal("removal returned before pending retry owner exited")
	}
	require.Empty(t, tr.connectionPool.GetPeerAddress(2))
	require.ErrorContains(t, tr.connectionPool.RestartConnection(2), "no connection for peer")
}

func TestDefaultTransport_InitialProbeFailureShutdownStopsRetry(t *testing.T) {
	t.Parallel()
	tr, _ := newInitialProbeTransport(t)
	tr.peersMu.RLock()
	peer := tr.peers[2]
	tr.peersMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, tr.Stop(ctx))
	select {
	case <-peer.loopDone:
	default:
		t.Fatal("shutdown returned before pending retry owner exited")
	}
	tr.AddPeer(3, "127.0.0.1:1")
	tr.peersMu.RLock()
	_, exists := tr.peers[3]
	tr.peersMu.RUnlock()
	require.False(t, exists, "shutdown must reject new retry owners")
	require.Empty(t, tr.connectionPool.PeerIDs())
}
