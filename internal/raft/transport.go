package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

type Incoming struct {
	Msg raftpb.Message
	Rsp chan error
}

// GRPCTransport handles network communication between Raft nodes using gRPC
// It wraps GRPCClientPool and manages Raft-specific message routing and channels
type GRPCTransport struct {
	UnimplementedRaftTransportServiceServer
	connectionPool *transport.ConnectionPool

	// Channel for Incoming messages
	recvCh chan Incoming

	// Channels for outgoing messages per peer
	peers map[uint64]peerConnection

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger logging.Logger
}

// NewTransport creates a new transport with a gRPC connection pool and client pool
func NewTransport(logger logging.Logger, connectionPool *transport.ConnectionPool) *GRPCTransport {
	return &GRPCTransport{
		connectionPool: connectionPool,
		recvCh:         make(chan Incoming, 100),
		peers:          make(map[uint64]peerConnection),
		unreachableCh:  make(chan uint64, 100),
		logger:         logger,
	}
}

// Stop stops the transport
func (t *GRPCTransport) Stop(ctx context.Context) error {
	t.logger.Infof("Stopping raft transport")
	for _, peerConnection := range t.peers {
		if err := peerConnection.stop(ctx); err != nil {
			return err
		}
	}

	if err := t.connectionPool.Close(); err != nil {
		return err
	}

	//close(t.recvCh)
	close(t.unreachableCh)

	return nil
}

// AddPeer adds a peer to the transport
func (t *GRPCTransport) AddPeer(id uint64, addr string) {
	if err := t.connectionPool.AddPeer(id, addr); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "addr": addr, "error": err}).Errorf("Failed to add peer to client pool")
		return
	}

	if _, exists := t.peers[id]; !exists {
		conn := peerConnection{
			sendCh:        make(chan raftpb.Message, 100),
			closeCh:       make(chan chan struct{}),
			unreachableCh: t.unreachableCh,
			connection:    t.connectionPool.GetConnection(id),
			logger:        t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id)}),
			peerID:        id,
		}
		t.peers[id] = conn

		go conn.loop()
	}
}

// Send sends a message to a peer
func (t *GRPCTransport) Send(peerID uint64, msg raftpb.Message) {
	peer, exists := t.peers[peerID]

	if exists {
		select {
		case peer.sendCh <- msg:
		default:
			t.logger.
				WithFields(map[string]any{
					"peer": fmt.Sprintf("%x", peerID),
					"type": msg.Type.String(),
				}).
				Errorf("Send channel full, dropping message")
		}
	} else {
		t.logger.
			WithFields(map[string]any{
				"peer": fmt.Sprintf("%x", peerID),
				"type": msg.Type.String(),
			}).
			Errorf("No send channel for peer, dropping message")
	}
}

// Recv returns the channel for receiving messages
func (t *GRPCTransport) Recv() <-chan Incoming {
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

// HandleStreamMessages handles client streaming gRPC connection for receiving messages
// This maintains a persistent connection to avoid frequent reconnections
// The server receives all messages and sends a single response at the end
func (t *GRPCTransport) StreamMessages(stream grpc.BidiStreamingServer[SendMessageRequest, SendMessageResponse]) error {

	// Receive all messages from the stream
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(req.Message); err != nil {
			if err := stream.Send(&SendMessageResponse{
				Error:     fmt.Sprintf("failed to unmarshal message: %v", err),
				RequestId: req.Id,
			}); err != nil {
				return err
			}
			continue
		}

		// Send message to recvCh for processing
		rspChan := make(chan error)
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case t.recvCh <- Incoming{
			Msg: msg,
			Rsp: rspChan,
		}:
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case ret := <-rspChan:
				if err := stream.Send(&SendMessageResponse{
					Success: ret == nil,
					Error: func() string {
						if ret != nil {
							return ret.Error()
						}
						return ""
					}(),
					RequestId: req.Id,
				}); err != nil {
					return err
				}
			}
		default:
			if err := stream.Send(&SendMessageResponse{
				Error:     "recv channel full, dropping message",
				RequestId: req.Id,
			}); err != nil {
				return err
			}
		}
	}
}

// RegisterRaftTransportService registers the RaftTransportService on the given gRPC server
func RegisterRaftTransportService(server *grpc.Server, transport *GRPCTransport) {
	transport.RegisterRaftService(server)
}

// RegisterRaftService registers the RaftTransportService on the given gRPC server
func (t *GRPCTransport) RegisterRaftService(server *grpc.Server) {
	RegisterRaftTransportServiceServer(server, t)
}

type peerConnection struct {
	sendCh        chan raftpb.Message
	closeCh       chan chan struct{}
	unreachableCh chan uint64
	connection    *grpc.ClientConn
	logger        logging.Logger
	peerID        uint64
}

func (conn *peerConnection) loop() {
	// Retry loop to reconnect if stream is closed

	messageID := uint64(0)
	for {
		select {
		case ch := <-conn.closeCh:
			close(ch)
			return
		default:
		}

		// Create client streaming connection
		client := NewRaftTransportServiceClient(conn.connection)
		stream, err := client.StreamMessages(context.Background())
		if err != nil {
			conn.logger.
				WithFields(map[string]any{
					"error": err,
				}).
				Errorf("Failed to create stream to peer")
			// Report peer as unreachable
			select {
			case conn.unreachableCh <- conn.peerID:
			default:
				conn.logger.Errorf("Unreachable channel full, dropping unreachable")
			}
			// Wait before retrying
			select {
			case ch := <-conn.closeCh:
				close(ch)
				return
			case <-time.After(300 * time.Millisecond): //todo: make configurable
			}
			continue
		}
		conn.logger.Infof("Created stream to peer")

		pending := make(map[uint64]uint64)
		mu := sync.Mutex{}
		go func() {
			for {
				res, err := stream.Recv()
				if err != nil {
					return
				}
				if !res.Success {
					mu.Lock()
					nodeID, ok := pending[res.RequestId]
					if ok {
						delete(pending, res.RequestId)
					}
					mu.Unlock()
					if ok {
						conn.unreachableCh <- nodeID
					}
				}
			}
		}()

	l:
		for {
			select {
			case ch := <-conn.closeCh:
				close(ch)
				return
			case msg := <-conn.sendCh:
				data, err := msg.Marshal()
				if err != nil {
					conn.logger.
						WithFields(map[string]any{
							"error": err,
						}).
						Errorf("Failed to marshal message")
					continue
				}

				conn.logger.
					WithFields(map[string]any{
						"type": msg.Type.String(),
					}).
					Debugf("Sending message to peer via stream")

				mu.Lock()
				pending[messageID] = msg.To
				mu.Unlock()
				messageID++

				if err := stream.Send(&SendMessageRequest{
					Message: data,
					Id:      messageID,
				}); err != nil {
					conn.logger.
						WithFields(map[string]any{
							"error": err,
						}).
						Errorf("Failed to send message via stream")
					// Report peer as unreachable
					select {
					case conn.unreachableCh <- msg.To:
					default:
						conn.logger.Errorf("Unreachable channel full, dropping unreachable")
					}
					break l
				}
			}
		}

		_ = stream.CloseSend()
	}
}

func (conn *peerConnection) stop(ctx context.Context) error {
	conn.logger.Infof("Stopping peer connection")
	ch := make(chan struct{})
	select {
	case conn.closeCh <- ch:
		select {
		case <-ch:
			close(conn.closeCh)
			close(conn.sendCh)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
