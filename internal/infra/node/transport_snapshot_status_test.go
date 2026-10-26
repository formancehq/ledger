package node

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	transportpkg "github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
)

type snapshotDeliveryServer struct {
	rafttransportpb.UnimplementedRaftTransportServiceServer

	mode       string
	attempts   atomic.Int32
	lowStarted chan struct{}
}

func (s *snapshotDeliveryServer) StreamMessages(stream grpc.BidiStreamingServer[rafttransportpb.SendMessageRequest, rafttransportpb.SendMessageResponse]) error {
	if priorities := metadata.ValueFromIncomingContext(stream.Context(), "priority"); len(priorities) > 0 && priorities[0] == "low" {
		s.lowStarted <- struct{}{}
	}
	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if ping := request.GetPing(); ping != nil {
			if err := stream.Send(&rafttransportpb.SendMessageResponse{Message: &rafttransportpb.SendMessageResponse_Pong{Pong: &rafttransportpb.PongResponse{SeqId: ping.GetSeqId()}}}); err != nil {
				return err
			}

			continue
		}
		responses := make([]*rafttransportpb.RaftResponseMessage, 0, len(request.GetRaft().GetMessages()))
		for _, message := range request.GetRaft().GetMessages() {
			m := &raftpb.Message{}
			if err := proto.Unmarshal(message.GetMessage(), m); err != nil {
				return err
			}
			success := true
			if m.GetType() == raftpb.MsgSnap && s.attempts.Add(1) == 1 {
				switch s.mode {
				case "rejected":
					success = false
				case "closed":
					return status.Error(codes.Unavailable, "test stream closed before acknowledgement")
				}
			}
			responses = append(responses, &rafttransportpb.RaftResponseMessage{RequestId: message.GetId(), Success: success})
		}
		if err := stream.Send(&rafttransportpb.SendMessageResponse{Message: &rafttransportpb.SendMessageResponse_Raft{Raft: &rafttransportpb.RaftResponseBatch{Messages: responses}}}); err != nil {
			return err
		}
	}
}

func TestTransportReportsSentSnapshotDelivery(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"accepted", "rejected", "closed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			peer := &snapshotDeliveryServer{mode: mode, lowStarted: make(chan struct{}, 4)}
			rafttransportpb.RegisterRaftTransportServiceServer(server, peer)
			go func() {
				// Cleanup owns shutdown; delivery assertions surface startup failures.
				_ = server.Serve(listener)
			}()
			t.Cleanup(server.Stop)
			pool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
			tr := NewTransport(logging.Testing(), pool, noop.NewMeterProvider(), 3,
				TransportConfig{Reception: []int{8, 8, 8}, Send: []int{8, 8, 8}}, "test", 1024, "", "")
			go tr.Start(t.Context())
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, tr.Stop(ctx))
			})
			tr.AddPeer(1, listener.Addr().String())
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			waitStream := func() {
				t.Helper()
				select {
				case <-peer.lowStarted:
				case <-ctx.Done():
					t.Fatal("peer stream did not start")
				}
			}
			waitStatus := func(expected raft.SnapshotStatus) {
				t.Helper()
				select {
				case result := <-tr.SnapshotStatus():
					require.Equal(t, SnapshotStatus{PeerID: 1, Status: expected}, result)
				case <-ctx.Done():
					t.Fatal("sent snapshot did not produce a delivery result")
				}
			}
			snapshot := []*raftpb.Message{{Type: new(raftpb.MsgSnap), From: proto.Uint64(3), To: proto.Uint64(1), Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(36), Term: proto.Uint64(2)}}}}
			waitStream()
			tr.Send(snapshot)
			if mode == "accepted" {
				waitStatus(raft.SnapshotFinish)
				require.Equal(t, int32(1), peer.attempts.Load())

				return
			}
			waitStatus(raft.SnapshotFailure)
			if mode == "closed" {
				waitStream()
			}
			tr.Send(snapshot)
			waitStatus(raft.SnapshotFinish)
			require.Equal(t, int32(2), peer.attempts.Load())
		})
	}
}

func TestTransportReportsSnapshotQueueDrops(t *testing.T) {
	t.Parallel()
	snapshot := []*raftpb.Message{{Type: new(raftpb.MsgSnap), To: proto.Uint64(2)}}
	t.Run("dispatcher queue", func(t *testing.T) {
		t.Parallel()
		tr := newTestTransport(t, 0)
		tr.snapshotStatusCh = make(chan SnapshotStatus, 1)
		tr.Send(snapshot)
		require.Equal(t, SnapshotStatus{PeerID: 2, Status: raft.SnapshotFailure}, <-tr.SnapshotStatus())
	})
	t.Run("peer queue", func(t *testing.T) {
		t.Parallel()
		results := make(chan SnapshotStatus, 1)
		conn := newTestPeerConn(t, 2, func(uint64) bool { return true })
		conn.stopCtx = t.Context()
		conn.reportSnapshotStatus = func(_ context.Context, peerID uint64, status raft.SnapshotStatus) {
			results <- SnapshotStatus{PeerID: peerID, Status: status}
		}
		require.False(t, conn.pushMessages(2, snapshot))
		require.Equal(t, SnapshotStatus{PeerID: 2, Status: raft.SnapshotFailure}, <-results)
	})
}

func TestSnapshotStatusBackpressureEndsOnCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := &DefaultTransport{snapshotStatusCh: make(chan SnapshotStatus), stopCh: make(chan struct{})}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			tr.reportSnapshotStatus(ctx, 1, raft.SnapshotFailure)
			close(done)
		}()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("snapshot result was dropped without a receiver")
		default:
		}
		cancel()
		synctest.Wait()
		<-done
	})
}
