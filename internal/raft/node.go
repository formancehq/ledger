package raft

import (
	"context"
	"fmt"
	"path/filepath"
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
type Node[State any, F FSM[State]] struct {
	node        *raft.RawNode
	logger      logging.Logger
	mu          sync.RWMutex
	futures     map[uint64]*applyFuture // Map of command ID -> future
	fsmSyncer   *syncer[State, F]
	storage     *WALStorage
	transport   NodeTransport
	config      NodeConfig
	stopChannel chan chan struct{}
}

// NewNode creates a new wrapper around a RawNode
func NewNode[State any, F FSM[State]](
	cfg NodeConfig,
	storage *WALStorage,
	transport NodeTransport,
	fsm F,
	logger logging.Logger,
) (*Node[State, F], error) {

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
		Logger:          NewLoggerAdapter(logger),
	}

	// Configure snapshot parameters
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}

	err := restoreFromStorage(fsm, storage, logger)
	if err != nil {
		return nil, fmt.Errorf("restoring FSM from storage: %w", err)
	}

	// Initialize storage with ConfState if storage is empty
	// This replaces the deprecated Bootstrap() method
	// All nodes need ConfState to participate in elections
	logger.Infof("Storage empty: %v", storage.IsEmpty())
	if storage.IsEmpty() {
		logger.Infof("Storage is empty - initializing with ConfState")

		// Build voters list (all peers including current node)
		voters := make([]uint64, 0, len(cfg.Peers)+1)
		voters = append(voters, cfg.NodeID)

		// Add peers if provided
		for _, peerEntry := range cfg.Peers {
			voters = append(voters, peerEntry.ID)
		}

		// Create ConfState with all voters
		confState := raftpb.ConfState{
			Voters: voters,
		}

		// Create initial snapshot with ConfState at index 0
		// This ensures FirstIndex() returns 1, which is the correct starting point
		// The ConfState in the snapshot defines the initial cluster configuration
		snapshotData, err := fsm.CreateSnapshot(context.Background())
		if err != nil {
			return nil, fmt.Errorf("creating initial snapshot data: %w", err)
		}

		// Create snapshot at index 0 with the ConfState
		// Index 0 means no entries yet, so FirstIndex() will return 1
		_, err = storage.CreateSnapshot(0, &confState, snapshotData)
		if err != nil {
			return nil, fmt.Errorf("creating initial snapshot: %w", err)
		}

		logger.WithFields(map[string]any{"voters": len(voters)}).Infof("Storage initialized with ConfState")
	}

	node, err := raft.NewRawNode(raftConfig)
	if err != nil {
		return nil, fmt.Errorf("creating raw node: %w", err)
	}

	spool, err := newSpool(filepath.Join(cfg.DataDir, "spool"))
	if err != nil {
		return nil, fmt.Errorf("creating spool: %w", err)
	}

	return &Node[State, F]{
		node:        node,
		logger:      logger,
		futures:     make(map[uint64]*applyFuture),
		fsmSyncer:   newSyncer[State, F](spool, fsm, logger),
		storage:     storage,
		transport:   transport,
		config:      cfg,
		stopChannel: make(chan chan struct{}),
	}, nil
}

func (node *Node[State, F]) Inner() F {
	return node.fsmSyncer.fsm
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node[State, F]) Apply(cmd *Command, timeout time.Duration) (uint64, any, error) {
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
func (node *Node[State, F]) NotifyApplied(commandID uint64, result any, index uint64, err error) {
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
func (node *Node[State, F]) readyLoop() {
	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	defer func() {
		_ = node.storage.Close()
	}()

	var (
		confState *raftpb.ConfState
	)
	for {
		// Process ticks and messages first, then Ready structures
		// Always check for ticks first (non-blocking) to ensure election timeouts work
		select {
		case <-ticker.C:
			if !node.fsmSyncer.syncing.Load() {
				node.node.Tick()
			}
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

		for node.node.HasReady() {
			rd := node.node.Ready()

			if len(rd.Entries) > 0 {
				if err := node.storage.Append(rd.Entries); err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to append entries")
					continue
				}
			}

			// Save HardState, Entries and Snapshot to storage
			if !raft.IsEmptyHardState(rd.HardState) {
				node.storage.SetHardState(rd.HardState)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				node.logger.
					WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).
					Infof("Applying snapshot sent from leader")

				if err := node.storage.ApplySnapshot(rd.Snapshot); err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to apply snapshot to storage")
					continue
				}

				node.node.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)
				node.fsmSyncer.RestoreSnapshot(context.Background(), rd.Snapshot.Data)
			}

			// Send messages via transport
			for _, msg := range rd.Messages {
				node.transport.Send(msg)
			}

			// Apply committed entries
			var (
				results        []ApplyResult
				commands       = make([]Command, 0, len(rd.CommittedEntries))
				commandIndexes = make([]uint64, 0, len(rd.CommittedEntries))
			)
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
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
				case raftpb.EntryConfChange:
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
					confState = node.node.ApplyConfChange(cc)
				case raftpb.EntryConfChangeV2:
					var cc raftpb.ConfChangeV2
					if err := cc.Unmarshal(entry.Data); err != nil {
						node.logger.
							WithFields(map[string]any{"error": err}).
							Errorf("Failed to unmarshal ConfChangeV2")
						continue
					}
					node.logger.
						WithFields(map[string]any{"transition": cc.Transition.String()}).
						Infof("Applying configuration change V2")
					// ApplyConfChange accepts ConfChangeI interface, which is implemented by both ConfChange and ConfChangeV2
					confState = node.node.ApplyConfChange(cc)
				}
			}

			// Apply bucket-specific entries to bucket FSM
			results = node.fsmSyncer.ApplyEntries(context.Background(), commands...)

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
				snapshotData, err := node.fsmSyncer.CreateSnapshot(context.Background())
				if err != nil {
					node.logger.
						WithFields(map[string]any{"error": err}).
						Errorf("Failed to create snapshot data")
					continue
				}

				// Get current ConfState from storage (use confState if available, otherwise get from storage)
				var currentConfState *raftpb.ConfState
				if confState != nil {
					currentConfState = confState
				} else {
					_, cs, err := node.storage.InitialState()
					if err != nil {
						node.logger.
							WithFields(map[string]any{"error": err}).
							Errorf("Failed to get ConfState from storage")
						continue
					}
					currentConfState = &cs
				}

				// Create snapshot in storage
				_, err = node.storage.CreateSnapshot(status.Applied, currentConfState, snapshotData)
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
func (node *Node[State, F]) Status() raft.Status {
	return node.node.Status()
}

func (node *Node[State, F]) Start() error {
	// Start the Ready loop - it will receive all messages and route them appropriately
	go node.readyLoop()

	return nil
}

func (node *Node[State, F]) IsLeader() bool {
	status := node.node.Status()
	return status.Lead == status.ID
}

func (node *Node[State, F]) GetLeader() uint64 {
	return node.node.Status().Lead
}

// GetClusterState returns the current state of the Raft cluster
func (node *Node[State, F]) GetClusterState(ctx context.Context) (*ledger.ClusterState[State], error) {
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

	return &ledger.ClusterState[State]{
		State:     stateStr,
		Leader:    uint(leaderID),
		Nodes:     nodes,
		LocalNode: uint(node.config.NodeID),
		InnerState: node.fsmSyncer.fsm.GetState(),
	}, nil
}

// Snapshot forces a snapshot of the Raft cluster
func (node *Node[State, F]) Snapshot(ctx context.Context) error {
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
		snapshotData, err := node.fsmSyncer.CreateSnapshot(ctx)
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
func (node *Node[State, F]) IsHealthy() bool {
	status := node.node.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

func (node *Node[State, F]) Stop(ctx context.Context) error {
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
func restoreFromStorage[State any](fsm FSM[State], storage *WALStorage, logger logging.Logger) error {
	logger.Infof("Restoring FSM from storage")
	// Read the last snapshot
	snapshot, err := storage.Snapshot()
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}

	// If snapshot exists, restore FSM from it
	if snapshot.Metadata.Index > 0 {
		logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
		fsm.RestoreSnapshot(context.Background(), snapshot.Data)
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
				if entry.Type == raftpb.EntryConfChange || entry.Type == raftpb.EntryConfChangeV2 {
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
