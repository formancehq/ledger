package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

// GRPCTransport handles network communication between Raft nodes using gRPC
// It wraps GRPCClientPool and manages Raft-specific message routing and channels
type GRPCTransport struct {
	connectionPool *transport.ConnectionPool

	// Channel for incoming messages
	recvCh chan raftpb.Message

	// Channels for outgoing messages per peer
	sendChs map[uint64]chan raftpb.Message

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger logging.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTransport creates a new transport with a gRPC connection pool and client pool
func NewTransport(logger logging.Logger, connectionPool *transport.ConnectionPool) *GRPCTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &GRPCTransport{
		connectionPool: connectionPool,
		recvCh:         make(chan raftpb.Message, 100),
		sendChs:        make(map[uint64]chan raftpb.Message),
		unreachableCh:  make(chan uint64, 100),
		logger:         logger,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Stop stops the transport
func (t *GRPCTransport) Stop() {
	t.cancel()
	close(t.recvCh)
	close(t.unreachableCh)
	for _, ch := range t.sendChs {
		close(ch)
	}
	t.connectionPool.Close()
}

// AddPeer adds a peer to the transport
func (t *GRPCTransport) AddPeer(id uint64, addr string) {
	if err := t.connectionPool.AddPeer(id, addr); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "addr": addr, "error": err}).Errorf("Failed to add peer to client pool")
		return
	}

	if _, exists := t.sendChs[id]; !exists {
		t.sendChs[id] = make(chan raftpb.Message, 100)
		go t.sendLoop(id, addr)
	}
}

// RemovePeer removes a peer from the transport
func (t *GRPCTransport) RemovePeer(id uint64) {
	t.connectionPool.RemovePeer(id)

	if ch, exists := t.sendChs[id]; exists {
		close(ch)
		delete(t.sendChs, id)
	}
}

// Send sends a message to a peer
func (t *GRPCTransport) Send(peerID uint64, msg raftpb.Message) {
	ch, exists := t.sendChs[peerID]

	if exists {
		select {
		case ch <- msg:
		case <-t.ctx.Done():
		default:
			t.logger.Infof("WARN: Send channel full, dropping message")
		}
	} else {
		t.logger.Infof("WARN: No send channel for peer, dropping message")
	}
}

// Recv returns the channel for receiving messages
func (t *GRPCTransport) Recv() <-chan raftpb.Message {
	return t.recvCh
}

// Unreachable returns the channel for reporting unreachable peers
func (t *GRPCTransport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// GetPeerConnection returns the gRPC connection for a specific peer, if it exists
// This allows reusing existing connections for service calls instead of creating new ones
func (t *GRPCTransport) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return t.connectionPool.GetConnection(peerID)
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (t *GRPCTransport) GetPeerAddress(peerID uint64) string {
	return t.connectionPool.GetPeerAddress(peerID)
}

// sendLoop sends messages to a peer using gRPC
func (t *GRPCTransport) sendLoop(peerID uint64, addr string) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-t.sendChs[peerID]:
			if !ok {
				return
			}

			// Send message via gRPC with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.logger.WithFields(map[string]any{"type": msg.Type.String(), "peer": fmt.Sprintf("%x", peerID), "addr": addr}).Debugf("Sending message to peer via gRPC")

			data, err := msg.Marshal()
			if err != nil {
				panic(err)
			}

			conn := NewRaftTransportServiceClient(t.connectionPool.GetConnection(peerID))
			if _, err := conn.SendMessage(ctx, &SendMessageRequest{
				Message:       data,
			}); err != nil {
				t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", peerID), "error": err}).Infof("WARN: Failed to send message via gRPC")
				cancel()
				// Report peer as unreachable
				select {
				case t.unreachableCh <- peerID:
				case <-t.ctx.Done():
					return
				default:
				}
				continue
			}
			cancel()
		}
	}
}

// HandleSendMessage handles unary gRPC calls for sending messages
// This method can be called from an external gRPC server
func (t *GRPCTransport) HandleSendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Message); err != nil {
		return &SendMessageResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal message: %v", err),
		}, nil
	}

	// Send message to recvChs for processing
	select {
	case t.recvCh <- msg:
		t.logger.
			WithFields(map[string]any{
				"type": msg.Type.String(),
				"from": fmt.Sprintf("%x", msg.From),
				"to": fmt.Sprintf("%x", msg.To),
			}).
			Debugf("Received message via gRPC")
		return &SendMessageResponse{Success: true}, nil
	case <-ctx.Done():
		return &SendMessageResponse{
			Success: false,
			Error:   "context cancelled",
		}, nil
	default:
		t.logger.Infof("WARN: Recv channel full, dropping message")
		return &SendMessageResponse{
			Success: false,
			Error:   "recv channel full",
		}, nil
	}
}

// RegisterRaftTransportService registers the RaftTransportService on the given gRPC server
func RegisterRaftTransportService(server *grpc.Server, transport *GRPCTransport) {
	transport.RegisterRaftService(server)
}

// RegisterRaftService registers the RaftTransportService on the given gRPC server
func (t *GRPCTransport) RegisterRaftService(server *grpc.Server) {
	grpcTransportServer := &grpcTransportServerWrapper{transport: t}
	RegisterRaftTransportServiceServer(server, grpcTransportServer)
}

// grpcTransportServerWrapper wraps the transport to implement RaftTransportServiceServer
type grpcTransportServerWrapper struct {
	UnimplementedRaftTransportServiceServer
	transport *GRPCTransport
}

func (s *grpcTransportServerWrapper) SendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	return s.transport.HandleSendMessage(ctx, req)
}
