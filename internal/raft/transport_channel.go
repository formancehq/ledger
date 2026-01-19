package raft

import (
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

// ChannelTransport implements the Transport interface using in-memory channels.
// This is useful for testing or in-process communication between Raft nodes.
type ChannelTransport struct {
	mu sync.RWMutex

	// nodeID is the ID of the local node
	nodeID uint64

	// recvCh receives incoming messages from other nodes
	recvCh chan raftpb.Message

	// unreachableCh reports peers that are unreachable
	unreachableCh chan uint64

	// peers maps peer IDs to their transports for direct channel communication
	peers map[uint64]*ChannelTransport

	// closed indicates if the transport has been closed
	closed bool
}

// ChannelTransportConfig holds configuration for the ChannelTransport
type ChannelTransportConfig struct {
	// RecvBufferSize is the buffer size for the receive channel
	RecvBufferSize int
	// UnreachableBufferSize is the buffer size for the unreachable channel
	UnreachableBufferSize int
}

// DefaultChannelTransportConfig returns a default configuration
func DefaultChannelTransportConfig() ChannelTransportConfig {
	return ChannelTransportConfig{
		RecvBufferSize:        1000,
		UnreachableBufferSize: 100,
	}
}

// NewChannelTransport creates a new channel-based transport
func NewChannelTransport(nodeID uint64, config ChannelTransportConfig) *ChannelTransport {
	return &ChannelTransport{
		nodeID:        nodeID,
		recvCh:        make(chan raftpb.Message, config.RecvBufferSize),
		unreachableCh: make(chan uint64, config.UnreachableBufferSize),
		peers:         make(map[uint64]*ChannelTransport),
	}
}

// Connect connects two transports for direct channel communication.
// This establishes a bidirectional link between the two nodes.
func (t *ChannelTransport) Connect(peer *ChannelTransport) {
	t.mu.Lock()
	t.peers[peer.nodeID] = peer
	t.mu.Unlock()

	peer.mu.Lock()
	peer.peers[t.nodeID] = t
	peer.mu.Unlock()
}

// Disconnect removes the connection to a peer
func (t *ChannelTransport) Disconnect(peerID uint64) {
	t.mu.Lock()
	peer, exists := t.peers[peerID]
	if exists {
		delete(t.peers, peerID)
	}
	t.mu.Unlock()

	if peer != nil {
		peer.mu.Lock()
		delete(peer.peers, t.nodeID)
		peer.mu.Unlock()
	}
}

// Send sends a message to the target peer.
// If the peer is not connected or the channel is full, the message is dropped
// and the peer is reported as unreachable.
func (t *ChannelTransport) Send(msg raftpb.Message) {
	t.mu.RLock()
	peer, exists := t.peers[msg.To]
	closed := t.closed
	t.mu.RUnlock()

	if closed {
		return
	}

	if !exists {
		// Peer not connected, report as unreachable
		select {
		case t.unreachableCh <- msg.To:
		default:
			// Unreachable channel full, drop
		}
		return
	}

	// Try to send to peer's receive channel
	peer.mu.RLock()
	peerClosed := peer.closed
	peer.mu.RUnlock()

	if peerClosed {
		select {
		case t.unreachableCh <- msg.To:
		default:
		}
		return
	}

	select {
	case peer.recvCh <- msg:
		// Message sent successfully
	default:
		// Peer's receive buffer is full, report as unreachable
		select {
		case t.unreachableCh <- msg.To:
		default:
		}
	}
}

// Recv returns the channel for receiving messages
func (t *ChannelTransport) Recv() <-chan raftpb.Message {
	return t.recvCh
}

// Unreachable returns the channel for reporting unreachable peers
func (t *ChannelTransport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// GetPeerConnection returns nil as channel transport doesn't use gRPC connections.
// This method exists to satisfy the Transport interface.
// For operations that require gRPC (like streaming logs), use a different transport.
func (t *ChannelTransport) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return nil
}

// Close closes the transport and all its channels
func (t *ChannelTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	t.closed = true
	close(t.recvCh)
	close(t.unreachableCh)

	// Disconnect from all peers
	for peerID, peer := range t.peers {
		peer.mu.Lock()
		delete(peer.peers, t.nodeID)
		peer.mu.Unlock()
		delete(t.peers, peerID)
	}
}

// NodeID returns the node ID of this transport
func (t *ChannelTransport) NodeID() uint64 {
	return t.nodeID
}

// IsConnected checks if a peer is connected
func (t *ChannelTransport) IsConnected(peerID uint64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.peers[peerID]
	return exists
}

// ConnectedPeers returns a list of connected peer IDs
func (t *ChannelTransport) ConnectedPeers() []uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	peers := make([]uint64, 0, len(t.peers))
	for peerID := range t.peers {
		peers = append(peers, peerID)
	}
	return peers
}

// Ensure ChannelTransport implements Transport interface
var _ Transport = (*ChannelTransport)(nil)
