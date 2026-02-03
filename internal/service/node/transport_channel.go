package node

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

	// 3 priority channels for incoming messages from other nodes
	priorityHigh chan []raftpb.Message // Heartbeats
	priorityMid  chan []raftpb.Message // Votes, responses
	priorityLow  chan []raftpb.Message // Data messages

	// unreachableCh reports peers that are unreachable
	unreachableCh chan uint64

	// peers maps peer IDs to their transports for direct channel communication
	peers map[uint64]*ChannelTransport

	// closed indicates if the transport has been closed
	closed bool
}

// ChannelTransportConfig holds configuration for the ChannelTransport
type ChannelTransportConfig struct {
	// RecvBufferSize is the buffer size for each priority receive channel (high, medium, low)
	RecvBufferSize [3]int
	// UnreachableBufferSize is the buffer size for the unreachable channel
	UnreachableBufferSize int
}

// DefaultChannelTransportConfig returns a default configuration
func DefaultChannelTransportConfig() ChannelTransportConfig {
	return ChannelTransportConfig{
		RecvBufferSize:        [3]int{1000, 1000, 1000},
		UnreachableBufferSize: 100,
	}
}

// NewChannelTransport creates a new channel-based transport
func NewChannelTransport(nodeID uint64, config ChannelTransportConfig) *ChannelTransport {
	return &ChannelTransport{
		nodeID:        nodeID,
		priorityHigh:  make(chan []raftpb.Message, config.RecvBufferSize[0]),
		priorityMid:   make(chan []raftpb.Message, config.RecvBufferSize[1]),
		priorityLow:   make(chan []raftpb.Message, config.RecvBufferSize[2]),
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

// Send sends messages to the target peers.
// If a peer is not connected or the channel is full, the message is dropped
// and the peer is reported as unreachable.
func (t *ChannelTransport) Send(msgs []raftpb.Message) {
	// Group messages by peer and priority
	msgsByPeerAndPriority := make(map[uint64]map[int][]raftpb.Message)

	for _, msg := range msgs {
		if _, exists := msgsByPeerAndPriority[msg.To]; !exists {
			msgsByPeerAndPriority[msg.To] = make(map[int][]raftpb.Message)
		}
		priority := channelMessagePriority(msg.Type)
		msgsByPeerAndPriority[msg.To][priority] = append(msgsByPeerAndPriority[msg.To][priority], msg)
	}

	// Send batches to each peer's priority channels
	for peerID, priorityMsgs := range msgsByPeerAndPriority {
		for priority, batch := range priorityMsgs {
			t.sendBatch(peerID, priority, batch)
		}
	}
}

// channelMessagePriority returns the priority level for a raft message type
func channelMessagePriority(msgType raftpb.MessageType) int {
	switch msgType {
	case raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp:
		return 0 // high
	case raftpb.MsgAppResp, raftpb.MsgVote, raftpb.MsgVoteResp, raftpb.MsgPreVote, raftpb.MsgPreVoteResp:
		return 1 // medium
	default:
		return 2 // low
	}
}

func (t *ChannelTransport) sendBatch(peerID uint64, priority int, msgs []raftpb.Message) {
	t.mu.RLock()
	peer, exists := t.peers[peerID]
	closed := t.closed
	t.mu.RUnlock()

	if closed {
		return
	}

	if !exists {
		// Peer not connected, report as unreachable
		select {
		case t.unreachableCh <- peerID:
		default:
			// Unreachable channel full, drop
		}
		return
	}

	// Try to send to peer's receive channel
	// Hold the lock during send to prevent race with Close()
	// This is safe because we use select with default, so we never block
	peer.mu.RLock()
	defer peer.mu.RUnlock()

	if peer.closed {
		select {
		case t.unreachableCh <- peerID:
		default:
		}
		return
	}

	var ch chan []raftpb.Message
	switch priority {
	case 0:
		ch = peer.priorityHigh
	case 1:
		ch = peer.priorityMid
	default:
		ch = peer.priorityLow
	}

	select {
	case ch <- msgs:
		// Message batch sent successfully
	default:
		// Peer's receive buffer is full, report as unreachable
		select {
		case t.unreachableCh <- peerID:
		default:
		}
	}
}

// RecvHighPriority returns the channel for receiving high priority messages (heartbeats)
func (t *ChannelTransport) RecvHighPriority() <-chan []raftpb.Message {
	return t.priorityHigh
}

// RecvMediumPriority returns the channel for receiving medium priority messages (votes, responses)
func (t *ChannelTransport) RecvMediumPriority() <-chan []raftpb.Message {
	return t.priorityMid
}

// RecvLowPriority returns the channel for receiving low priority messages (data)
func (t *ChannelTransport) RecvLowPriority() <-chan []raftpb.Message {
	return t.priorityLow
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
	close(t.priorityHigh)
	close(t.priorityMid)
	close(t.priorityLow)
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
