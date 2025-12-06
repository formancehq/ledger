package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Transport handles network communication between Raft nodes using gRPC
type Transport struct {
	id       uint64
	addr     string
	listener net.Listener
	peers    map[uint64]string // peer ID -> address
	mu       sync.RWMutex

	// Channel for incoming messages
	recvCh chan raftpb.Message

	// Channels for outgoing messages per peer
	sendChs map[uint64]chan raftpb.Message

	// gRPC clients for peers
	grpcClients map[uint64]*grpcClient

	// gRPC server (defined in transport_grpc.go, type is *grpc.Server)
	grpcServer interface {
		GracefulStop()
		Stop()
	}

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTransport creates a new transport
func NewTransport(id uint64, addr string, logger *zap.Logger) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		id:            id,
		addr:          addr,
		peers:         make(map[uint64]string),
		recvCh:        make(chan raftpb.Message, 100),
		sendChs:       make(map[uint64]chan raftpb.Message),
		grpcClients:   make(map[uint64]*grpcClient),
		unreachableCh: make(chan uint64, 100),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start starts the gRPC transport server
func (t *Transport) Start() error {
	return t.startGRPCServer()
}

// Stop stops the transport
func (t *Transport) Stop() {
	t.cancel()
	t.stopGRPCServer()
	if t.listener != nil {
		t.listener.Close()
	}
	close(t.recvCh)
	close(t.unreachableCh)
	for _, ch := range t.sendChs {
		close(ch)
	}
	// Close all gRPC client connections
	t.mu.Lock()
	for _, client := range t.grpcClients {
		client.close()
	}
	t.mu.Unlock()
}

// AddPeer adds a peer to the transport
func (t *Transport) AddPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[id] = addr
	if _, exists := t.sendChs[id]; !exists {
		t.sendChs[id] = make(chan raftpb.Message, 100)
		// Create gRPC client for this peer
		client, err := newGRPCClient(addr)
		if err != nil {
			t.logger.Error("Failed to create gRPC client for peer", zap.String("peer", fmt.Sprintf("%x", id)), zap.String("addr", addr), zap.Error(err))
			return
		}
		t.grpcClients[id] = client
		go t.sendLoop(id, addr)
	}
}

// RemovePeer removes a peer from the transport
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, id)
	if ch, exists := t.sendChs[id]; exists {
		close(ch)
		delete(t.sendChs, id)
	}
	// Close and remove gRPC client
	if client, exists := t.grpcClients[id]; exists {
		client.close()
		delete(t.grpcClients, id)
	}
}

// Send sends a message to a peer
func (t *Transport) Send(msg raftpb.Message) {
	t.mu.RLock()

	// Determine the target peer ID for routing
	// If msg.To >= 0x10000, it's a bucket group message, extract the global node ID
	targetPeerID := msg.To
	if msg.To >= 0x10000 {
		// Extract global node ID from bucket group node ID
		// groupNodeID = groupID + nodeID, where groupID = bucketID << 16
		// So nodeID = groupNodeID & 0xFFFF (lower 16 bits)
		targetPeerID = msg.To & 0xFFFF
	}

	ch, exists := t.sendChs[targetPeerID]
	t.mu.RUnlock()

	if exists {
		select {
		case ch <- msg:
		case <-t.ctx.Done():
		default:
			t.logger.Warn("Send channel full, dropping message",
				zap.String("to", fmt.Sprintf("%x", msg.To)),
				zap.String("targetPeerID", fmt.Sprintf("%x", targetPeerID)))
		}
	} else {
		t.logger.Warn("No send channel for peer, dropping message",
			zap.String("to", fmt.Sprintf("%x", msg.To)),
			zap.String("targetPeerID", fmt.Sprintf("%x", targetPeerID)))
	}
}

// Recv returns the channel for receiving messages
func (t *Transport) Recv() <-chan raftpb.Message {
	return t.recvCh
}

// Unreachable returns the channel for reporting unreachable peers
func (t *Transport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// sendLoop sends messages to a peer using gRPC
func (t *Transport) sendLoop(peerID uint64, addr string) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-t.sendChs[peerID]:
			if !ok {
				return
			}

			// Get gRPC client for this peer
			t.mu.RLock()
			client, exists := t.grpcClients[peerID]
			t.mu.RUnlock()

			if !exists {
				t.logger.Warn("No gRPC client for peer", zap.String("peer", fmt.Sprintf("%x", peerID)))
				// Report peer as unreachable
				select {
				case t.unreachableCh <- peerID:
				case <-t.ctx.Done():
					return
				default:
				}
				continue
			}

			// Send message via gRPC with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.logger.Debug("Sending message to peer via gRPC",
				zap.String("type", msg.Type.String()),
				zap.String("peer", fmt.Sprintf("%x", peerID)),
				zap.String("addr", addr))
			if err := client.sendMessage(ctx, msg); err != nil {
				t.logger.Warn("Failed to send message via gRPC", zap.String("peer", fmt.Sprintf("%x", peerID)), zap.Error(err))
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
