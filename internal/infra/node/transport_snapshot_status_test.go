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
	"go.uber.org/mock/gomock"
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
			if s.attempts.Add(1) == 1 {
				switch s.mode {
				case "rejected":
					success = false
				case "closed":
					return status.Error(codes.Unavailable, "test stream closed before acknowledgement")
				}
			}
			responses = append(responses, &rafttransportpb.RaftResponseMessage{RequestId: message.GetId(), Success: success})
		}
		if s.mode == "duplicate" {
			responses = append(responses, responses...)
		}
		if err := stream.Send(&rafttransportpb.SendMessageResponse{Message: &rafttransportpb.SendMessageResponse_Raft{Raft: &rafttransportpb.RaftResponseBatch{Messages: responses}}}); err != nil {
			return err
		}
	}
}

func TestTransportReportsMessageDelivery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		messageType raftpb.MessageType
		mode        string
	}{
		{"snapshot duplicate", raftpb.MsgSnap, "duplicate"},
		{"heartbeat duplicate", raftpb.MsgHeartbeat, "duplicate"},
		{"snapshot accepted", raftpb.MsgSnap, "accepted"},
		{"snapshot rejected", raftpb.MsgSnap, "rejected"},
		{"snapshot closed", raftpb.MsgSnap, "closed"},
		{"heartbeat accepted", raftpb.MsgHeartbeat, "accepted"},
		{"heartbeat rejected", raftpb.MsgHeartbeat, "rejected"},
		{"heartbeat closed", raftpb.MsgHeartbeat, "closed"},
	} {
		mode := tc.mode
		t.Run(tc.name, func(t *testing.T) {
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
			waitStatus := func(expected bool) {
				t.Helper()
				select {
				case result := <-tr.DeliveryResults():
					require.Equal(t, DeliveryResult{PeerID: 1, Type: tc.messageType, Success: expected}, result)
				case <-ctx.Done():
					t.Fatal("sent snapshot did not produce a delivery result")
				}
			}
			snapshot := []*raftpb.Message{{Type: new(tc.messageType), From: proto.Uint64(3), To: proto.Uint64(1), Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(36), Term: proto.Uint64(2)}}}}
			waitStream()
			tr.Send(snapshot, t.Context().Done())
			if mode == "accepted" || mode == "duplicate" {
				waitStatus(true)
				require.Equal(t, int32(1), peer.attempts.Load())
				if mode == "duplicate" {
					tr.Send([]*raftpb.Message{msg(raftpb.MsgApp, 3, 1)}, t.Context().Done())
					select {
					case result := <-tr.DeliveryResults():
						require.Equal(t, DeliveryResult{PeerID: 1, Type: raftpb.MsgApp, Success: true}, result, "duplicate acknowledgement must not publish twice")
					case <-ctx.Done():
						t.Fatal("sentinel delivery did not complete")
					}
				}

				return
			}
			waitStatus(false)
			if mode == "closed" {
				waitStream()
			}
			tr.Send(snapshot, t.Context().Done())
			waitStatus(true)
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
		tr.deliveryResultsCh = make(chan DeliveryResult, 1)
		tr.Send(snapshot, t.Context().Done())
		require.Equal(t, DeliveryResult{PeerID: 2, Type: raftpb.MsgSnap, Success: false}, <-tr.DeliveryResults())
	})
	t.Run("peer queue", func(t *testing.T) {
		t.Parallel()
		results := make(chan DeliveryResult, 1)
		conn := newTestPeerConn(t, 2, func(uint64) bool { return true })
		conn.stopCtx = t.Context()
		conn.reportDelivery = func(_ <-chan struct{}, result DeliveryResult) {
			results <- result
		}
		require.False(t, conn.pushMessages(2, snapshot))
		require.Equal(t, DeliveryResult{PeerID: 2, Type: raftpb.MsgSnap, Success: false}, <-results)
	})
}

func TestSnapshotStatusBackpressureEndsOnCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := &DefaultTransport{deliveryResultsCh: make(chan DeliveryResult), stopCh: make(chan struct{})}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			tr.reportDelivery(ctx.Done(), DeliveryResult{PeerID: 1, Type: raftpb.MsgSnap})
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

func TestSnapshotQueueRejectionPreservesBackpressure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := newTestTransport(t, 0)
		tr.deliveryResultsCh = make(chan DeliveryResult, 1)
		previous := DeliveryResult{PeerID: 2, Type: raftpb.MsgSnap, Success: true}
		tr.deliveryResultsCh <- previous
		done := make(chan struct{})
		go func() {
			tr.Send([]*raftpb.Message{msg(raftpb.MsgSnap, 1, 3)}, t.Context().Done())
			close(done)
		}()
		synctest.Wait()
		require.False(t, shutdownComplete(done), "queue rejection must wait for status capacity")
		require.Equal(t, previous, <-tr.DeliveryResults())
		synctest.Wait()
		require.True(t, shutdownComplete(done))
		require.Equal(t, DeliveryResult{PeerID: 3, Type: raftpb.MsgSnap, Success: false}, <-tr.DeliveryResults())
	})
}

func TestReadySnapshotBackpressureEndsBeforeTransportShutdown(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		tr := newTestTransport(t, 0) // Every batch is rejected by the dispatcher queue.
		tr.stopCh = make(chan struct{})
		tr.deliveryResultsCh = make(chan DeliveryResult, 100)
		for range cap(tr.deliveryResultsCh) {
			tr.deliveryResultsCh <- DeliveryResult{PeerID: 2, Type: raftpb.MsgSnap, Success: false}
		}

		// Keep status consumption disabled to model an orchestrator that has
		// stopped. Send still traverses the real Ready worker and transport.
		transport := NewMockTransport(gomock.NewController(t))
		transport.EXPECT().Unreachable().AnyTimes().Return((<-chan uint64)(nil))
		transport.EXPECT().DeliveryResults().AnyTimes().Return((<-chan DeliveryResult)(nil))
		transport.EXPECT().RecvHighPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
		transport.EXPECT().RecvMediumPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
		transport.EXPECT().RecvLowPriority().AnyTimes().Return((<-chan []*raftpb.Message)(nil))
		entered, sent := make(chan struct{}), make(chan struct{})
		transport.EXPECT().Send(gomock.Any(), gomock.Any()).Do(func(msgs []*raftpb.Message, stop <-chan struct{}) {
			close(entered)
			tr.Send(msgs, stop)
			close(sent)
		})
		n, result, resume := startShutdownNodeWithTransport(t, t.Context(), newTestApplierSetup(t), transport)
		// Rescue the unfixed implementation before fixture cleanup joins Run.
		t.Cleanup(func() { close(tr.stopCh) })
		n.readies <- raft.Ready{Messages: []*raftpb.Message{msg(raftpb.MsgSnap, 1, 2)}}
		<-entered
		synctest.Wait()
		require.False(t, shutdownComplete(sent), "Ready must preserve snapshot result backpressure")

		stopped := make(chan error, 1)
		go func() { stopped <- n.Stop(context.Background()) }()
		<-n.stopChannel
		resume()
		synctest.Wait()
		require.True(t, shutdownComplete(n.runDone), "Node.Run must join Ready before the transport stop hook")
		require.True(t, shutdownComplete(sent), "Ready-owned publisher must observe task stop")
		require.False(t, shutdownComplete(tr.stopCh), "transport must remain live throughout the node join")
		require.NoError(t, <-stopped)
		require.NoError(t, <-result)
		require.Len(t, tr.deliveryResultsCh, 100, "shutdown must not depend on draining status results")
	})
}
