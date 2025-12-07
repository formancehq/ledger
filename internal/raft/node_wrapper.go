package raft

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/commands"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// applyFuture represents a future for an applied entry
type applyFuture struct {
	index  uint64
	ch     chan error
	result any
	mu     sync.Mutex
	done   bool
	err    error
}

// NodeWrapper wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type NodeWrapper struct {
	node    *raft.RawNode
	logger  logging.Logger
	mu      sync.RWMutex
	futures map[uint64]*applyFuture // Map of command ID -> future
}

// NewNodeWrapper creates a new wrapper around a RawNode
func NewNodeWrapper(node *raft.RawNode, logger logging.Logger) *NodeWrapper {
	return &NodeWrapper{
		node:    node,
		logger:  logger,
		futures: make(map[uint64]*applyFuture),
	}
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (n *NodeWrapper) Apply(cmd *commands.Command, timeout time.Duration) (uint64, any, error) {
	// Serialize the command to binary format
	cmdData, err := cmd.MarshalBinary()
	if err != nil {
		return 0, nil, err
	}

	// Create a future for this application using command ID as key
	future := &applyFuture{
		index: 0, // Will be set when entry is applied
		ch:    make(chan error, 1),
	}

	// Register the future using command ID
	n.mu.Lock()
	n.futures[cmd.ID] = future
	n.mu.Unlock()

	// Propose the command
	if err := n.node.Propose(cmdData); err != nil {
		// Clean up the future
		n.mu.Lock()
		delete(n.futures, cmd.ID)
		n.mu.Unlock()
		return 0, nil, err
	}

	// Wait for the future to complete with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case err := <-future.ch:
		n.mu.Lock()
		delete(n.futures, cmd.ID)
		n.mu.Unlock()
		if err != nil {
			return 0, nil, err
		}
		return future.index, future.result, nil
	case <-ctx.Done():
		// Timeout - clean up the future
		n.mu.Lock()
		delete(n.futures, cmd.ID)
		n.mu.Unlock()
		return 0, nil, ctx.Err()
	}
}

// NotifyApplied notifies the wrapper that a command with the given ID has been applied
// This should be called from the readyLoop when entries are applied
func (n *NodeWrapper) NotifyApplied(commandID uint64, result any, index uint64, err error) {
	n.mu.RLock()
	future, exists := n.futures[commandID]
	n.mu.RUnlock()

	if !exists {
		return
	}

	future.mu.Lock()
	if !future.done {
		future.done = true
		future.index = index
		future.result = result
		future.err = err
		// Send error (or nil) to channel
		select {
		case future.ch <- err:
		default:
			// Channel already closed or error already sent
		}
	}
	future.mu.Unlock()
}

// RawNode returns the underlying RawNode for direct access when needed
func (n *NodeWrapper) RawNode() *raft.RawNode {
	return n.node
}

// Bootstrap bootstraps the cluster with the given peers
func (n *NodeWrapper) Bootstrap(peers []raft.Peer) error {
	return n.node.Bootstrap(peers)
}

// Status returns the current status of the node
func (n *NodeWrapper) Status() raft.Status {
	return n.node.Status()
}

// ReportUnreachable reports that the given peer is unreachable
func (n *NodeWrapper) ReportUnreachable(id uint64) {
	n.node.ReportUnreachable(id)
}

// Tick advances the internal logical clock by a single tick
func (n *NodeWrapper) Tick() {
	n.node.Tick()
}

// Step advances the state machine using the given message
func (n *NodeWrapper) Step(msg raftpb.Message) error {
	return n.node.Step(msg)
}

// Ready returns the current Ready state
func (n *NodeWrapper) Ready() raft.Ready {
	return n.node.Ready()
}

// HasReady returns true if there is a Ready state available
func (n *NodeWrapper) HasReady() bool {
	return n.node.HasReady()
}

// Advance notifies that the node has processed the Ready state
func (n *NodeWrapper) Advance(rd raft.Ready) {
	n.node.Advance(rd)
}

// ApplyConfChange applies a configuration change
func (n *NodeWrapper) ApplyConfChange(cc raftpb.ConfChange) *raftpb.ConfState {
	return n.node.ApplyConfChange(cc)
}
