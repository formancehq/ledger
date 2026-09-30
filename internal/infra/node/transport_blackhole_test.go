package node

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	transportpkg "github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

type pingServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer

	pings        chan struct{}
	raftMessages chan struct{}
}

func (s *pingServer) StreamMessages(stream grpc.BidiStreamingServer[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if ping := request.GetPing(); ping != nil {
			select {
			case s.pings <- struct{}{}:
			default:
			}
			if err := stream.Send(&rafttransportpb.SendMessageResponse{Message: &rafttransportpb.SendMessageResponse_Pong{Pong: &rafttransportpb.PongResponse{SeqId: ping.GetSeqId()}}}); err != nil {
				return err
			}
		} else if batch := request.GetRaft(); batch != nil {
			responses := make([]*rafttransportpb.RaftResponseMessage, 0, len(batch.GetMessages()))
			for _, message := range batch.GetMessages() {
				responses = append(responses, &rafttransportpb.RaftResponseMessage{RequestId: message.GetId(), Success: true})
				select {
				case s.raftMessages <- struct{}{}:
				default:
				}
			}
			if err := stream.Send(&rafttransportpb.SendMessageResponse{Message: &rafttransportpb.SendMessageResponse_Raft{Raft: &rafttransportpb.RaftResponseBatch{Messages: responses}}}); err != nil {
				return err
			}
		}
	}
}

// A TCP proxy that silently drops both directions after the first successful
// application ping reproduces an old-IP connection with no FIN or RST.
func TestPeerConnectionDetectsSilentTCPBlackhole(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pinged := make(chan struct{}, 1)
	rafttransportpb.RegisterRaftTransportServiceServer(server, &pingServer{pings: pinged})
	go func() { _ = server.Serve(backend) }()
	t.Cleanup(server.Stop)
	replacement, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	replacementServer := grpc.NewServer()
	replacementPinged := make(chan struct{}, 1)
	replacementRaft := make(chan struct{}, 1)
	rafttransportpb.RegisterRaftTransportServiceServer(replacementServer, &pingServer{pings: replacementPinged, raftMessages: replacementRaft})
	go func() { _ = replacementServer.Serve(replacement) }()
	t.Cleanup(replacementServer.Stop)

	front, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var blackhole atomic.Bool
	var connectionsMu sync.Mutex
	var connections []net.Conn
	acceptDone := make(chan struct{})
	t.Cleanup(func() {
		_ = front.Close()
		<-acceptDone
		connectionsMu.Lock()
		defer connectionsMu.Unlock()
		for _, connection := range connections {
			_ = connection.Close()
		}
	})
	go func() {
		defer close(acceptDone)
		for {
			client, err := front.Accept()
			if err != nil {
				return
			}
			stale := !blackhole.Load()
			target := replacement.Addr().String()
			if stale {
				target = backend.Addr().String()
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()

				return
			}
			connectionsMu.Lock()
			connections = append(connections, client, upstream)
			connectionsMu.Unlock()
			forward := func(dst, src net.Conn) {
				buf := make([]byte, 32*1024)
				for {
					n, err := src.Read(buf)
					if err != nil {
						return
					}
					if !stale || !blackhole.Load() {
						if _, err := dst.Write(buf[:n]); err != nil {
							return
						}
					}
				}
			}
			go forward(upstream, client)
			go forward(client, upstream)
		}
	}()

	pool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(2, front.Addr().String()))
	tr := NewTransport(logging.Testing(), pool, noop.NewMeterProvider(), 1,
		TransportConfig{Reception: []int{4, 4, 4}, Send: []int{4, 4, 4}}, "test", 1, "", "")
	conn := newTestPeerConn(t, 2, func(uint64) bool { return true })
	conn.connectionPool = pool
	conn.stopCtx, conn.stopCancel = context.WithCancel(context.Background())
	conn.nodeID = 1
	conn.clusterID = "test"
	conn.bufferSize = 1024
	conn.loopDone = make(chan struct{})
	conn.logger = tr.logger
	conn.pingLatency, err = tr.meterProvider.Meter("test").Int64Histogram("ping")
	require.NoError(t, err)
	conn.pendingResponseCounter, err = tr.meterProvider.Meter("test").Float64UpDownCounter("pending")
	require.NoError(t, err)
	t.Cleanup(func() { conn.stopCancel(); <-conn.loopDone })
	go conn.loop()
	select {
	case <-pinged:
	case <-time.After(6 * time.Second):
		t.Fatal("initial ping not delivered")
	}
	blackhole.Store(true)
	select {
	case <-replacementPinged:
	case <-time.After(12 * time.Second):
		t.Fatal("silent blackhole prevented reconnecting to the replacement endpoint")
	}
	go func() { conn.highPriorityCh <- []*raftpb.Message{{Type: new(raftpb.MsgHeartbeat), To: new(uint64(2))}} }()
	select {
	case <-replacementRaft:
	case <-time.After(3 * time.Second):
		t.Fatal("new endpoint did not receive a Raft message")
	}
	conn.stopCancel()
	select {
	case <-conn.loopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("peer loop did not stop")
	}
}

func TestConnectionLivenessRejectsStaleAndDuplicatePongs(t *testing.T) {
	t.Parallel()
	liveness := newConnectionLiveness()
	first, ok := liveness.beginProbe()
	require.True(t, ok)
	_, ok = liveness.beginProbe()
	require.False(t, ok, "only one probe may be outstanding")
	_, ok = liveness.acceptPong(first + 1)
	require.False(t, ok)
	require.True(t, liveness.expired(time.Now().Add(transportLivenessTimeout+time.Second)),
		"an unmatched pong cannot renew liveness")
	_, ok = liveness.acceptPong(first)
	require.True(t, ok)
	_, ok = liveness.acceptPong(first)
	require.False(t, ok, "a duplicate pong cannot renew liveness")
	second, ok := liveness.beginProbe()
	require.True(t, ok)
	require.Greater(t, second, first)
	_, ok = liveness.acceptPong(first)
	require.False(t, ok, "a delayed prior pong cannot satisfy the new probe")
	require.True(t, liveness.expired(time.Now().Add(transportLivenessTimeout+time.Second)))
}

func TestConnectionLivenessDetectsBlockedSendAndProbeStarvation(t *testing.T) {
	t.Parallel()
	liveness := newConnectionLiveness()
	liveness.beginSend()
	require.True(t, liveness.expired(time.Now().Add(transportLivenessTimeout+time.Second)))
	liveness.endSend()
	require.True(t, liveness.expired(time.Now().Add(transportLivenessTimeout+time.Second)),
		"continuous successful sends cannot suppress the probe deadline")
}

// A full send queue must not prevent probes to a responsive peer. Otherwise
// the no-probe watchdog would tear down healthy replication under load.
func TestPeerConnectionProbesDuringSustainedTraffic(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	pinged := make(chan struct{}, 16)
	rafttransportpb.RegisterRaftTransportServiceServer(server, &pingServer{pings: pinged})
	go func() { _ = server.Serve(backend) }()
	t.Cleanup(server.Stop)

	pool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(2, backend.Addr().String()))
	original := pool.GetConnection(2)
	tr := NewTransport(logging.Testing(), pool, noop.NewMeterProvider(), 1,
		TransportConfig{Reception: []int{4, 4, 4}, Send: []int{64, 4, 4}}, "test", 1, "", "")
	conn := newTestPeerConn(t, 2, func(uint64) bool { return true })
	const backlogSize = 5_000_000
	conn.highPriorityCh = make(chan []*raftpb.Message, backlogSize)
	conn.connectionPool = pool
	conn.stopCtx, conn.stopCancel = context.WithCancel(context.Background())
	conn.loopDone = make(chan struct{})
	conn.nodeID = 1
	conn.clusterID = "test"
	conn.bufferSize = 1024
	conn.logger = tr.logger
	conn.pingLatency, err = tr.meterProvider.Meter("test").Int64Histogram("ping")
	require.NoError(t, err)
	conn.pendingResponseCounter, err = tr.meterProvider.Meter("test").Float64UpDownCounter("pending")
	require.NoError(t, err)
	t.Cleanup(func() { conn.stopCancel(); <-conn.loopDone })
	go conn.loop()
	select {
	case <-pinged:
	case <-time.After(6 * time.Second):
		t.Fatal("initial ping not delivered")
	}

	message := []*raftpb.Message{{Type: new(raftpb.MsgHeartbeat), To: new(uint64(2))}}
	for range cap(conn.highPriorityCh) {
		conn.highPriorityCh <- message
	}
	conn.sendQueueInflight[0].Store(backlogSize)
	require.Eventually(t, func() bool { return len(pinged) >= 6 }, 11*time.Second, 10*time.Millisecond,
		"sustained traffic must not starve application probes")
	require.NotEmpty(t, conn.highPriorityCh, "the send queue must remain backlogged throughout the probe window")
	require.Same(t, original, pool.GetConnection(2), "healthy traffic must not restart the connection")
}
