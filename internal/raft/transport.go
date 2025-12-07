package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger-v3-poc/internal/transport"
)

// Transport handles network communication between Raft nodes using gRPC
// It wraps GRPCClientPool and manages Raft-specific message routing and channels
type Transport struct {
	clientPool *GRPCClientPool

	// Channel for incoming messages
	recvCh chan raftpb.Message

	// Channels for outgoing messages per peer
	sendChs map[uint64]chan raftpb.Message

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger logging.Logger
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
}

// NewTransport creates a new transport with a gRPC connection pool and client pool
func NewTransport(logger logging.Logger, connectionPool *transport.ConnectionPool) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	clientPool := NewGRPCClientPool(connectionPool)
	return &Transport{
		clientPool:    clientPool,
		recvCh:        make(chan raftpb.Message, 100),
		sendChs:       make(map[uint64]chan raftpb.Message),
		unreachableCh: make(chan uint64, 100),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Stop stops the transport
func (t *Transport) Stop() {
	t.cancel()
	close(t.recvCh)
	close(t.unreachableCh)
	t.mu.Lock()
	for _, ch := range t.sendChs {
		close(ch)
	}
	t.mu.Unlock()
	t.clientPool.Close()
}

// AddPeer adds a peer to the transport
func (t *Transport) AddPeer(id uint64, addr string) {
	if err := t.clientPool.AddPeer(id, addr); err != nil {
		t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "addr": addr, "error": err}).Errorf("Failed to add peer to client pool")
		return
	}

	t.mu.Lock()
	if _, exists := t.sendChs[id]; !exists {
		t.sendChs[id] = make(chan raftpb.Message, 100)
		go t.sendLoop(id, addr)
	}
	t.mu.Unlock()
}

// RemovePeer removes a peer from the transport
func (t *Transport) RemovePeer(id uint64) {
	t.clientPool.RemovePeer(id)

	t.mu.Lock()
	if ch, exists := t.sendChs[id]; exists {
		close(ch)
		delete(t.sendChs, id)
	}
	t.mu.Unlock()
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
			t.logger.WithFields(map[string]any{"to": fmt.Sprintf("%x", msg.To), "targetPeerID": fmt.Sprintf("%x", targetPeerID)}).Infof("WARN: Send channel full, dropping message")
		}
	} else {
		t.logger.WithFields(map[string]any{"to": fmt.Sprintf("%x", msg.To), "targetPeerID": fmt.Sprintf("%x", targetPeerID)}).Infof("WARN: No send channel for peer, dropping message")
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

// GetPeerConnection returns the gRPC connection for a specific peer, if it exists
// This allows reusing existing connections for service calls instead of creating new ones
func (t *Transport) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return t.clientPool.GetPeerConnection(peerID)
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (t *Transport) GetPeerAddress(peerID uint64) (string, bool) {
	return t.clientPool.GetPeerAddress(peerID)
}

// GetConnectionPool returns the underlying gRPC connection pool
// This allows reusing connections for other services
func (t *Transport) GetConnectionPool() *transport.ConnectionPool {
	return t.clientPool.GetConnectionPool()
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

			// Send message via gRPC with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.logger.WithFields(map[string]any{"type": msg.Type.String(), "peer": fmt.Sprintf("%x", peerID), "addr": addr}).Debugf("Sending message to peer via gRPC")
			if err := t.clientPool.SendMessage(ctx, peerID, msg); err != nil {
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
