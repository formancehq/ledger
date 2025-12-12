package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type NodeTransport interface {
	Send(msg raftpb.Message)
	Recv() <-chan raftpb.Message
	Unreachable() <-chan uint64
	GetPeerAddress(peerID uint64) string
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node[F FSM] struct {
	node        *raft.RawNode
	logger      logging.Logger
	mu          sync.RWMutex
	futures     map[uint64]*applyFuture // Map of command ID -> future
	fsm         F
	storage     *WALStorage
	transport   NodeTransport
	config      NodeConfig
	stopChannel chan chan struct{}
}

// NewNode creates a new wrapper around a RawNode
func NewNode[F FSM](
	cfg NodeConfig,
	storage *WALStorage,
	transport NodeTransport,
	fsm F,
	logger logging.Logger,
) (*Node[F], error) {
	// Set defaults if not configured
	electionTick := cfg.ElectionTick
	if electionTick == 0 {
		electionTick = 10
	}
	heartbeatTick := cfg.HeartbeatTick
	if heartbeatTick == 0 {
		heartbeatTick = 1
	}
	maxSizePerMsg := cfg.MaxSizePerMsg
	if maxSizePerMsg == 0 {
		maxSizePerMsg = 1024 * 1024 // 1MB
	}
	maxInflightMsgs := cfg.MaxInflightMsgs
	if maxInflightMsgs == 0 {
		maxInflightMsgs = 256
	}

	raftConfig := &raft.Config{
		ID:              cfg.NodeID,
		ElectionTick:    electionTick,
		HeartbeatTick:   heartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   maxSizePerMsg,
		MaxInflightMsgs: maxInflightMsgs,
	}

	// Configure snapshot parameters
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}

	err := restoreFromStorage(fsm, storage, logger)
	if err != nil {
		return nil, fmt.Errorf("restoring FSM from storage: %w", err)
	}

	node, err := raft.NewRawNode(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("creating raw node: %w", err)
	}

	// Build peers list if bootstrap and storage is empty
	logger.Infof("Bootstrap: %v, Storage empty: %v", cfg.Bootstrap, storage.IsEmpty())
	if cfg.Bootstrap && storage.IsEmpty() {
		logger.Infof("Bootstrap started")
		peers := make([]raft.Peer, 0, len(cfg.Peers)+1)
		peers = append(peers, raft.Peer{ID: cfg.NodeID})

		// Add peers if provided
		// Peers are in format "<id>/<address>", parse them
		for _, peerEntry := range cfg.Peers {
			peers = append(peers, raft.Peer{ID: peerEntry.ID})
		}

		// Bootstrap the cluster
		if err := node.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("bootstrapping cluster: %w", err)
		}
		logger.WithFields(map[string]any{"peers": len(peers)}).Infof("Node bootstrapped")
	}

	return &Node[F]{
		node:        node,
		logger:      logger,
		futures:     make(map[uint64]*applyFuture),
		fsm:         fsm,
		storage:     storage,
		transport:   transport,
		config:      cfg,
		stopChannel: make(chan chan struct{}),
	}, nil
}

func (node *Node[F]) Inner() F {
	return node.fsm
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node[F]) Apply(cmd *Command, timeout time.Duration) (uint64, any, error) {
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
	node.mu.Lock()
	node.futures[cmd.ID] = future
	node.mu.Unlock()

	// Propose the command
	if err := node.node.Propose(cmdData); err != nil {
		// Clean up the future
		node.mu.Lock()
		delete(node.futures, cmd.ID)
		node.mu.Unlock()
		return 0, nil, err
	}

	// Wait for the future to complete with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case err := <-future.ch:
		node.mu.Lock()
		delete(node.futures, cmd.ID)
		node.mu.Unlock()
		if err != nil {
			return 0, nil, err
		}
		return future.index, future.result, nil
	case <-ctx.Done():
		// Timeout - clean up the future
		node.mu.Lock()
		delete(node.futures, cmd.ID)
		node.mu.Unlock()
		return 0, nil, ctx.Err()
	}
}

// NotifyApplied notifies the wrapper that a command with the given ID has been applied
// This should be called from the readyLoop when entries are applied
func (node *Node[F]) NotifyApplied(commandID uint64, result any, index uint64, err error) {
	node.mu.RLock()
	future, exists := node.futures[commandID]
	node.mu.RUnlock()

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

// readyLoop processes Ready structures from etcd/raft for this bucket group with a specific message channel
func (node *Node[F]) readyLoop() {
	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {

		select {
		case <-ticker.C:
			node.node.Tick()
		case ch := <-node.stopChannel:
			close(ch)
			return
		case nodeID := <-node.transport.Unreachable():
			node.node.ReportUnreachable(nodeID)
		case msg := <-node.transport.Recv():
			if err := node.node.Step(msg); err != nil {
				panic(err)
			}
		}

		// Process Ready structures
		for node.node.HasReady() {
			rd := node.node.Ready()

			// Save HardState, Entries and Snapshot to storage
			if !raft.IsEmptyHardState(rd.HardState) {
				node.storage.SetHardState(rd.HardState)
			}

			if len(rd.Entries) > 0 {
				if err := node.storage.Append(rd.Entries); err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to append entries")
					continue
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				node.logger.
					WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).
					Infof("Applying snapshot")

				if err := node.storage.ApplySnapshot(rd.Snapshot); err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to apply snapshot to storage")
					continue
				}
				// Restore bucket FSM from snapshot
				if err := node.fsm.RestoreSnapshot(context.Background(), rd.Snapshot.Data); err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to restore bucket FSM from snapshot")
					node.node.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFailure)
					continue
				}

				node.node.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)
			}

			// Send messages via transport
			for _, msg := range rd.Messages {
				node.transport.Send(msg)
			}

			// Apply committed entries
			// todo: batch insertions
			var (
				results        []ApplyResult
				commands       = make([]Command, 0, len(rd.CommittedEntries))
				commandIndexes = make([]uint64, 0, len(rd.CommittedEntries))
			)
			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						node.logger.
							WithFields(map[string]any{"error": err}).
							Errorf("Failed to unmarshal ConfChange")
						continue
					}
					node.logger.
						WithFields(map[string]any{"type": cc.Type.String(), "nodeID": fmt.Sprintf("%x", cc.NodeID)}).
						Infof("Applying configuration change")
					node.node.ApplyConfChange(cc)
					continue
				}

				if entry.Type != raftpb.EntryNormal {
					node.logger.
						WithFields(map[string]any{"index": entry.Index, "type": uint64(entry.Type)}).
						Debugf("Skipping non-normal entry")
					continue
				}
				// Skip empty entries (they might be used for heartbeat or other Raft internal purposes)
				if len(entry.Data) == 0 {
					node.logger.
						WithFields(map[string]any{"index": entry.Index}).
						Debugf("Skipping empty entry")
					continue
				}

				// Decode the command to get its ID
				var cmd Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					node.logger.
						WithFields(map[string]any{"index": entry.Index, "error": err}).
						Errorf("Failed to unmarshal command for notification")
					continue
				}

				commands = append(commands, cmd)
				commandIndexes = append(commandIndexes, entry.Index)
			}

			// Apply bucket-specific entries to bucket FSM
			results = node.fsm.ApplyEntries(context.Background(), commands...)

			for i, result := range results {
				node.NotifyApplied(commands[i].ID, result.Result, commandIndexes[i], result.Error)
				if result.Error != nil {
					node.logger.
						WithFields(map[string]any{
							"error":     result.Error,
							"index":     commandIndexes[i],
							"commandID": commands[i].ID,
						}).
						Errorf("Failed to apply entry to bucket FSM")
				}
			}

			// Advance the node
			node.node.Advance(rd)

			// Check if we need to create a snapshot (every 1000 entries or when log is getting large)
			status := node.node.Status()
			if status.Applied > 0 && status.Applied%node.config.SnapshotThreshold == 0 {
				// Create snapshot: write logs to store and create snapshot data
				snapshotData, err := node.fsm.CreateSnapshot(context.Background())
				if err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to create snapshot data")
					continue
				}

				// Get current configuration state from storage
				_, confState, err := node.storage.InitialState()
				if err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to get initial state for snapshot")
					continue
				}

				// Create snapshot in storage
				_, err = node.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
				if err != nil {
					// Check if error is ErrSnapOutOfDate (expected if snapshot was already created)
					if err != ErrSnapOutOfDate {
						node.logger.
							WithFields(map[string]any{"error": err}).
							Errorf("Failed to create snapshot in storage")
					}
					// ErrSnapOutOfDate is expected if snapshot was already created
					continue
				}

				node.logger.Infof("Snapshot created for bucket")
			}
		}
	}
}

// Status returns the current status of the node
func (node *Node[F]) Status() raft.Status {
	return node.node.Status()
}

func (node *Node[F]) Start() error {
	// Start the Ready loop - it will receive all messages and route them appropriately
	go node.readyLoop()

	return nil
}

func (node *Node[F]) IsLeader() bool {
	status := node.node.Status()
	return status.Lead == status.ID
}

func (node *Node[F]) GetLeader() uint64 {
	return node.node.Status().Lead
}

// GetClusterState returns the current state of the Raft cluster
func (node *Node[F]) GetClusterState(ctx context.Context) (*ledger.ClusterState, error) {
	status := node.node.Status()

	// Get leader
	leaderID := status.Lead

	// Convert state to string
	stateStr := "Unknown"
	switch status.RaftState {
	case raft.StateLeader:
		stateStr = "Leader"
	case raft.StateFollower:
		stateStr = "Follower"
	case raft.StateCandidate:
		stateStr = "Candidate"
	case raft.StatePreCandidate:
		stateStr = "PreCandidate"
	}

	// Build nodes list from progress
	nodes := make([]ledger.NodeInfo, 0)
	for id := range status.Progress {
		suffrage := "Voter"

		var address string
		// todo: set address
		//if id == node.config.NodeID {
		//	address = node.config.AdvertiseAddr
		//} else {
		//	address = node.transport.GetPeerAddress(id)
		//}

		nodes = append(nodes, ledger.NodeInfo{
			ID:       uint(id),
			Address:  address,
			Suffrage: suffrage,
		})
	}

	return &ledger.ClusterState{
		State:     stateStr,
		Leader:    uint(leaderID),
		Nodes:     nodes,
		LocalNode: uint(node.config.NodeID),
	}, nil
}

// Snapshot forces a snapshot of the Raft cluster
func (node *Node[F]) Snapshot(ctx context.Context) error {
	node.logger.Info("Snapshot request received")

	// Check if we are the leader (only leader can create snapshots)
	status := node.node.Status()
	if status.RaftState != raft.StateLeader {
		node.logger.WithFields(map[string]any{"state": status.RaftState.String()}).Infof("WARN: Snapshot requested but not leader")
		return fmt.Errorf("only leader can create snapshots, current state: %v", status.RaftState)
	}

	node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Creating snapshot")

	// Trigger snapshot creation
	// In etcd/raft, snapshots are created automatically when needed
	// We can trigger one manually by checking the status
	if status.Applied > 0 {
		node.logger.WithFields(map[string]any{"applied": status.Applied}).Debugf("Creating snapshot data via FSM")
		// Create snapshot data via FSM
		snapshotData, err := node.fsm.CreateSnapshot(ctx)
		if err != nil {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot data")
			return fmt.Errorf("creating snapshot data: %w", err)
		}
		node.logger.WithFields(map[string]any{"size": len(snapshotData)}).Debugf("Snapshot data created")

		// Get current configuration state from storage
		node.logger.Debugf("Getting initial state from storage")
		_, confState, err := node.storage.InitialState()
		if err != nil {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to get initial state")
			return fmt.Errorf("getting initial state: %w", err)
		}

		// Create snapshot via storage
		node.logger.WithFields(map[string]any{"index": status.Applied}).Debugf("Creating snapshot in storage")
		_, err = node.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
		if err != nil {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot in storage")
			return fmt.Errorf("creating snapshot: %w", err)
		}

		node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Snapshot created successfully")
	} else {
		node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("WARN: No applied entries to snapshot")
	}
	return nil
}

// IsHealthy returns true if the node is connected to the cluster (leader or follower)
func (node *Node[F]) IsHealthy() bool {
	status := node.node.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

func (node *Node[F]) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case node.stopChannel <- ch:
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (node *Node[F]) Bootstrap(peers []raft.Peer) error {
	return node.node.Bootstrap(peers)
}

// applyFuture represents a future for an applied entry
type applyFuture struct {
	index  uint64
	ch     chan error
	result any
	mu     sync.Mutex
	done   bool
	err    error
}

// RestoreFromStorage restores the FSM state from storage by reading the last snapshot
// and applying all entries after the snapshot
func restoreFromStorage(fsm FSM, storage *WALStorage, logger logging.Logger) error {
	logger.Infof("Restoring FSM from storage")
	// Read the last snapshot
	snapshot, err := storage.Snapshot()
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}

	// If snapshot exists, restore FSM from it
	if snapshot.Metadata.Index > 0 {
		logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
		if err := fsm.RestoreSnapshot(context.Background(), snapshot.Data); err != nil {
			return fmt.Errorf("restoring FSM from snapshot: %w", err)
		}
	} else {
		logger.Infof("No snapshot found, starting with empty FSM")
	}

	// Read all entries after the snapshot
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return fmt.Errorf("getting first index: %w", err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		return fmt.Errorf("getting last index: %w", err)
	}

	// If there are entries after the snapshot, apply them to the FSM
	if firstIndex <= lastIndex {
		logger.WithFields(map[string]any{"firstIndex": firstIndex, "lastIndex": lastIndex}).Infof("Applying entries after snapshot")
		// Read entries in batches to avoid loading everything in memory at once
		const maxBatchSize = 1000
		for i := firstIndex; i <= lastIndex; i += maxBatchSize {
			endIndex := i + maxBatchSize
			if endIndex > lastIndex+1 {
				endIndex = lastIndex + 1
			}

			entries, err := storage.Entries(i, endIndex, 10*1024*1024) // 10MB max size per batch
			if err != nil {
				return fmt.Errorf("reading entries [%d, %d): %w", i, endIndex, err)
			}

			// Apply each entry to the FSM
			for _, entry := range entries {
				// Skip configuration change entries
				if entry.Type == raftpb.EntryConfChange {
					continue
				}
				// Skip other non-normal entries
				if entry.Type != raftpb.EntryNormal {
					continue
				}
				// Skip empty entries
				if len(entry.Data) == 0 {
					continue
				}

				// Decode the command
				var cmd Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Infof("WARN: Failed to unmarshal command during FSM restoration")
					continue
				}

				if ret := fsm.ApplyEntries(context.Background(), cmd); ret[0].Error != nil {
					logger.
						WithFields(map[string]any{
							"index":     entry.Index,
							"error":     ret[0].Error,
							"commandID": cmd.ID,
						}).
						Infof("WARN: Failed to apply entry during FSM restoration")
				}
			}
		}
		logger.WithFields(map[string]any{"lastIndex": lastIndex}).Infof("Finished applying entries after snapshot")
	}

	return nil
}
