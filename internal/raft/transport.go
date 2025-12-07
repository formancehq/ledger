package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

// Transport handles network communication between Raft nodes using gRPC
type Transport struct {
	peers map[uint64]string // peer ID -> address
	mu    sync.RWMutex

	// Channel for incoming messages
	recvCh chan raftpb.Message

	// Channels for outgoing messages per peer
	sendChs map[uint64]chan raftpb.Message

	// gRPC clients for peers
	grpcClients map[uint64]*grpcClient

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger logging.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTransport creates a new transport
func NewTransport(logger logging.Logger) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
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

// Stop stops the transport
func (t *Transport) Stop() {
	t.cancel()
	// Note: gRPC server is managed via fx hooks in application/module.go
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
			t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", id), "addr": addr, "error": err}).Errorf("Failed to create gRPC client for peer")
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	if client, exists := t.grpcClients[peerID]; exists {
		return client.getConnection()
	}
	return nil
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
				t.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", peerID)}).Infof("WARN: No gRPC client for peer")
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
			t.logger.WithFields(map[string]any{"type": msg.Type.String(), "peer": fmt.Sprintf("%x", peerID), "addr": addr}).Debugf("Sending message to peer via gRPC")
			if err := client.sendMessage(ctx, msg); err != nil {
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
