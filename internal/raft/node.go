package raft

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"
)

type NodeTransport interface {
	Send(msg raftpb.Message)
	Recv() <-chan raftpb.Message
	Unreachable() <-chan uint64
	GetPeerAddress(peerID uint64) string
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node[State any, F FSM[State]] struct {
	rawNode        *raft.RawNode
	logger         logging.Logger
	mu             sync.RWMutex
	syncer         *syncer[State, F]
	storage        *WALStorage
	transport      NodeTransport
	config         NodeConfig
	stopped        chan struct{}
	ctx            context.Context
	cancel         func()
	proposeCh      Queue[[]byte]
	confState      *raftpb.ConfState
	futures        map[uint64]*applyFuture // Map of command ID -> future
	lastSoftState  *raft.SoftState
	isSnapshotting *atomic.Bool

	meter                          metric.Meter
	applyEntriesHistogram          metric.Float64Histogram
	applyEntriesBatchSizeCounter   metric.Int64Counter
	applyEntriesBatchSizeHistogram metric.Int64Histogram
	leadMonitor                    metric.Int64Gauge
}

// NewNode creates a new wrapper around a RawNode
func NewNode[State any, F FSM[State]](
	cfg NodeConfig,
	storage *WALStorage,
	transport NodeTransport,
	fsm F,
	logger logging.Logger,
	meter metric.Meter,
) (*Node[State, F], error) {

	// Set defaults if not configured
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = 1024 * 1024 // 1MB
	}
	if cfg.MaxInflightMsgs == 0 {
		cfg.MaxInflightMsgs = 256
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}

	spool, err := newFileSpool(filepath.Join(cfg.DataDir, "spool"))
	if err != nil {
		return nil, fmt.Errorf("creating spool: %w", err)
	}

	node := &Node[State, F]{
		logger:    logger,
		meter:     meter,
		futures:   make(map[uint64]*applyFuture),
		syncer:    newSyncer[State, F](spool, fsm, logger, storage, meter),
		storage:   storage,
		transport: transport,
		config:    cfg,
		proposeCh: NewQueueObserver[[]byte](
			"raft.node.propose",
			NewSimpleQueue[[]byte](),
			WithLogger[[]byte](logger),
			WithMeter[[]byte](meter),
		),
		isSnapshotting: &atomic.Bool{},
	}
	go node.syncer.run()

	node.applyEntriesHistogram, err = meter.Float64Histogram("raft.apply_entries.duration",
		metric.WithDescription("Time spent applying entries to FSM"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			// Fine-grained buckets for small values (0-100ms)
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			12, 15, 18, 20, 25, 30, 35, 40, 45, 50,
			60, 70, 80, 90, 100,
			// Medium buckets (100-500ms)
			125, 150, 175, 200, 250, 300, 350, 400, 450, 500,
		),
	)
	if err != nil {
		panic(err)
	}

	// Create inflightCounter metric for ApplyEntries batch size
	node.applyEntriesBatchSizeCounter, err = meter.Int64Counter("raft.apply_entries.batch_size",
		metric.WithDescription("Size of batches passed to ApplyEntries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

	// Create histogram metric for ApplyEntries batch size distribution
	// Buckets: 1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000+
	node.applyEntriesBatchSizeHistogram, err = meter.Int64Histogram("raft.apply_entries.batch_size_distribution",
		metric.WithDescription("Distribution of batch sizes passed to ApplyEntries"),
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.leadMonitor, err = meter.Int64Gauge("raft.node.lead")
	if err != nil {
		panic(err)
	}

	return node, nil
}

func (node *Node[State, F]) Inner() F {
	return node.syncer.fsm
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node[State, F]) Apply(cmd *Command, timeout time.Duration) (uint64, any, error) {

	// Create a future for this application using command ID as key
	future := &applyFuture{
		index: 0, // Will be set when entry is applied
		ch:    make(chan error, 1),
	}

	// Register the future using command ID
	node.mu.Lock()
	node.futures[cmd.ID] = future
	node.mu.Unlock()

	cmdData, err := cmd.MarshalBinary()
	if err != nil {
		return 0, nil, fmt.Errorf("marshaling command: %w", err)
	}

	// Wait for the future to complete with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Propose the command
	if !node.proposeCh.Send(cmdData) {
		return 0, nil, fmt.Errorf("propose channel full")
	}

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

	defer otlplogs.RecoverAndLogPanics(node.logger)

	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	defer func() {
		_ = node.storage.Close()
	}()

	_, initialConfState, err := node.storage.InitialState()
	if err != nil {
		panic(err)
	}
	node.confState = &initialConfState

	processingTick := time.NewTicker(tickInterval / 20) // todo: make configurable

	for {
		select {
		case <-ticker.C:
			// Prevent election timeouts from happening while syncing the FSM
			if !node.syncer.IsSyncing() {
				node.rawNode.Tick()
			}
		case <-node.ctx.Done():
			node.logger.Infof("Stopping readyLoop as context was cancelled")
			close(node.stopped)
			return
		case nodeID := <-node.transport.Unreachable():
			node.logger.Errorf("Node %x is unreachable", nodeID)
			node.rawNode.ReportUnreachable(nodeID)
		case msg := <-node.transport.Recv():
			if err := node.rawNode.Step(msg); err != nil {
				panic(err)
			}
		case cmd := <-node.proposeCh.Recv():
			// todo: handle raft propose dropped, indicating the cluster has no leader
			// need to propagate ErrNoLeader to the caller?
			if err := node.rawNode.Propose(cmd); err != nil {
				panic(err)
			}
		case <-processingTick.C:
			if node.rawNode.HasReady() {
				err := node.processReady(node.ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}
			}
		}
	}
}

func (node *Node[State, F]) processReady(ctx context.Context) error {

	node.logger.Debugf("Processing ready")
	rd := node.rawNode.Ready()

	if rd.SoftState != nil {
		ss := rd.SoftState
		if node.lastSoftState != nil {
			status := node.rawNode.Status()

			// leadership loss
			if node.lastSoftState.RaftState == raft.StateLeader && ss.RaftState != raft.StateLeader {
				node.logger.
					WithFields(map[string]any{
						"lead": ss.Lead,
						"term": status.Term,
					}).
					Infof("Leadership lost")
			}
			// acquire leadership
			if node.lastSoftState.RaftState != raft.StateLeader && ss.RaftState == raft.StateLeader {
				node.logger.
					WithFields(map[string]any{
						"lead": ss.Lead,
						"term": status.Term,
					}).
					Infof("Leadership gained")
			}
		}
		node.leadMonitor.Record(ctx, int64(ss.Lead))
		node.lastSoftState = ss
	}

	if len(rd.Entries) > 0 {
		if err := node.storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("appending entries to storage: %w", err)
		}
	}

	// Save HardState, Entries and Snapshot to storage
	if !raft.IsEmptyHardState(rd.HardState) {
		node.storage.SetHardState(rd.HardState)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		node.logger.
			WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).
			Infof("Applying snapshot sent by leader")

		if err := node.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("applying snapshot to storage: %w", err)
		}

		node.rawNode.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)

		go func() {
			// todo: since the snapshot is already written in storage at this point
			// we must be able to detect a crash and restart the restoration process
			// in case of rawNode recover
			if err := node.syncer.RestoreSnapshot(context.Background(), node.lastSoftState.Lead, rd.Snapshot); err != nil {
				panic(fmt.Errorf("restoring snapshot in storage: %w", err))
			}
		}()
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
				return fmt.Errorf("unmarshaling command: %w", err)
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
			node.confState = node.rawNode.ApplyConfChange(cc)
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
			node.confState = node.rawNode.ApplyConfChange(cc)
		}
	}

	// Apply bucket-specific entries to bucket FSM
	// Measure time spent in ApplyEntries
	start := time.Now()
	results, err := node.syncer.ApplyEntries(node.ctx, commands...)
	duration := time.Since(start)

	batchSize := int64(len(commands))

	// Record metric if histogram is available and we have commands
	if node.applyEntriesHistogram != nil && batchSize > 0 {
		node.applyEntriesHistogram.Record(ctx, float64(duration.Milliseconds()))
	}

	// Record batch size inflightCounter if available
	if node.applyEntriesBatchSizeCounter != nil && batchSize > 0 {
		node.applyEntriesBatchSizeCounter.Add(ctx, batchSize)
	}

	// Record batch size histogram if available
	if node.applyEntriesBatchSizeHistogram != nil && batchSize > 0 {
		node.applyEntriesBatchSizeHistogram.Record(ctx, batchSize)
	}

	if err != nil {
		return fmt.Errorf("applying entries to FSM: %w", err)
	}

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

	// Advance the rawNode
	node.rawNode.Advance(rd)

	// Check if we need to create a snapshot
	lastSnapshot, err := node.storage.Snapshot()
	if err != nil {
		return fmt.Errorf("getting last snapshot: %w", err)
	}
	status := node.rawNode.Status()

	if status.Applied-lastSnapshot.Metadata.Index > node.config.SnapshotThreshold && !node.isSnapshotting.Swap(true) {

		node.logger.WithFields(map[string]any{
			"applied":           status.Applied,
			"term":              status.Term,
			"commit":            status.Commit,
			"snapshotThreshold": node.config.SnapshotThreshold,
		}).Infof("Creating new snapshot for ledger")

		go func() {
			defer node.isSnapshotting.Store(false)

			err := node.syncer.CreateSnapshot(ctx, status.Applied, node.confState)
			if err != nil {
				// Check if error is ErrSnapOutOfDate (expected if snapshot was already created)
				if errors.Is(err, ErrSnapOutOfDate) {
					node.logger.
						WithFields(map[string]any{"applied": status.Applied}).
						Infof("Snapshot already up to date, creating skippde")
					return
				}
				if errors.Is(err, context.Canceled) {
					node.logger.
						WithFields(map[string]any{"applied": status.Applied}).
						Infof("Snapshot creation cancelled")
					return
				}

				panic(fmt.Errorf("creating snapshot in storage: %w", err))
			}

			// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
			err = node.storage.Compact(status.Applied - node.config.CompactionMargin)
			if err != nil {
				panic("Compacting storage failed: " + err.Error())
			}

			node.logger.Infof("Snapshot created successfully")
		}()
	}

	return nil
}

// Status returns the current status of the rawNode
func (node *Node[State, F]) Status() raft.Status {
	return node.rawNode.Status()
}

func (node *Node[State, F]) Start(ctx context.Context) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.stopped = make(chan struct{})
	node.ctx, node.cancel = context.WithCancel(context.Background())

	// Initialize storage with ConfState if storage is empty
	// All nodes need ConfState to participate in elections
	node.logger.Infof("Storage empty: %v", node.storage.IsEmpty())
	if node.storage.IsEmpty() {
		node.logger.Infof("Storage is empty - initializing with ConfState")

		// Build voters list (all peers including current rawNode)
		voters := make([]uint64, 0, len(node.config.Peers)+1)
		voters = append(voters, node.config.NodeID)

		// Add peers if provided
		for _, peerEntry := range node.config.Peers {
			voters = append(voters, peerEntry.ID)
		}

		// Create ConfState with all voters
		confState := raftpb.ConfState{
			Voters: voters,
		}

		// Create initial snapshot with ConfState at index 0
		// This ensures FirstIndex() returns 1, which is the correct starting point
		// The ConfState in the snapshot defines the initial cluster configuration
		err := node.syncer.CreateSnapshot(ctx, 0, &confState)
		if err != nil {
			return fmt.Errorf("creating initial snapshot data: %w", err)
		}

		node.logger.WithFields(map[string]any{"voters": len(voters)}).Infof("Storage initialized with ConfState")
	} else {
		node.logger.Infof("Restoring FSM from storage")
		// Read the last snapshot
		snapshot, err := node.storage.Snapshot()
		if err != nil {
			return fmt.Errorf("reading snapshot: %w", err)
		}

		// If snapshot exists, restore FSM from it
		if snapshot.Metadata.Index > 0 {
			node.logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
			if err := node.syncer.fsm.RestoreSnapshot(context.Background(), 0, snapshot); err != nil {
				panic(err)
			}
			node.logger.Infof("Snapshot restored successfully")
		} else {
			node.logger.Infof("No snapshot found, starting with empty FSM")
		}

		node.logger.Infof("Finished restoring FSM from storage")
	}

	raftConfig := &raft.Config{
		ID: node.config.NodeID,
		// todo: add random delay on election tick
		ElectionTick:              node.config.ElectionTick,
		HeartbeatTick:             node.config.HeartbeatTick,
		Storage:                   node.storage,
		MaxSizePerMsg:             node.config.MaxSizePerMsg,
		MaxInflightMsgs:           node.config.MaxInflightMsgs,
		Logger:                    NewLoggerAdapter(node.logger),
		DisableProposalForwarding: true,
	}

	var err error
	node.rawNode, err = raft.NewRawNode(raftConfig)
	if err != nil {
		return fmt.Errorf("creating raw rawNode: %w", err)
	}

	// Start the Ready loop - it will receive all messages and route them appropriately
	go node.readyLoop()

	return nil
}

func (node *Node[State, F]) IsLeader() bool {
	status := node.rawNode.Status()
	return status.Lead == status.ID
}

func (node *Node[State, F]) GetLeader() uint64 {
	return node.rawNode.Status().Lead
}

// GetClusterState returns the current state of the Raft cluster
func (node *Node[State, F]) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[State], error) {
	status := node.rawNode.Status()

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
	nodes := make([]ledgerpb.NodeInfo, 0)
	for id := range status.Progress {
		suffrage := "Voter"

		var address string
		// todo: set address
		//if id == rawNode.config.NodeID {
		//	address = rawNode.config.AdvertiseAddr
		//} else {
		//	address = rawNode.transport.GetPeerAddress(id)
		//}

		nodes = append(nodes, ledgerpb.NodeInfo{
			ID:       uint(id),
			Address:  address,
			Suffrage: suffrage,
		})
	}

	// Build progress information map
	progress := make(map[uint64]ledgerpb.ProgressInfo)
	for id, prog := range status.Progress {
		// Convert StateType to string
		stateStr := "Unknown"
		switch prog.State {
		case tracker.StateProbe:
			stateStr = "Probe"
		case tracker.StateReplicate:
			stateStr = "Replicate"
		case tracker.StateSnapshot:
			stateStr = "Snapshot"
		}

		progress[id] = ledgerpb.ProgressInfo{
			Match:           prog.Match,
			Next:            prog.Next,
			State:           stateStr,
			PendingSnapshot: prog.PendingSnapshot,
			RecentActive:    prog.RecentActive,
			ProbeSent:       prog.ProbeSent,
			IsPaused:        prog.IsPaused(),
		}
	}

	// Get HardState for Term, Commit, Vote
	hardState, _, err := node.storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("getting initial state: %w", err)
	}

	// Get last index from storage
	lastIndex, err := node.storage.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("getting last index: %w", err)
	}

	// Build complete Raft status
	raftStatus := &ledgerpb.RaftStatus{
		State:     stateStr,
		Term:      hardState.Term,
		Leader:    leaderID,
		Applied:   status.Applied,
		Commit:    hardState.Commit,
		LastIndex: lastIndex,
		Vote:      hardState.Vote,
		Progress:  progress,
	}

	return &ledgerpb.ClusterState[State]{
		State:      stateStr,
		Leader:     uint(leaderID),
		Nodes:      nodes,
		LocalNode:  uint(node.config.NodeID),
		RaftStatus: raftStatus,
		InnerState: node.syncer.fsm.GetState(),
	}, nil
}

// Snapshot forces a snapshot of the Raft cluster
func (node *Node[State, F]) Snapshot(ctx context.Context) error {
	node.logger.Info("Snapshot request received")

	// Check if we are the leader (only leader can create snapshots)
	status := node.rawNode.Status()
	if status.RaftState != raft.StateLeader {
		return fmt.Errorf("only leader can create snapshots, current state: %v", status.RaftState)
	}

	node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Creating snapshot")

	// Trigger snapshot creation
	// In etcd/raft, snapshots are created automatically when needed
	// We can trigger one manually by checking the status
	if status.Applied > 0 {
		node.logger.WithFields(map[string]any{"applied": status.Applied}).Debugf("Creating snapshot data via FSM")

		// Get current configuration state from storage
		node.logger.Debugf("Getting initial state from storage")
		_, confState, err := node.storage.InitialState()
		if err != nil {
			return fmt.Errorf("getting initial state: %w", err)
		}

		// Create snapshot data via FSM
		err = node.syncer.CreateSnapshot(ctx, 0, &confState)
		if err != nil {
			return fmt.Errorf("creating snapshot data: %w", err)
		}

		node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Snapshot created successfully")
	} else {
		node.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("WARN: No applied entries to snapshot")
	}
	return nil
}

// IsHealthy returns true if the rawNode is connected to the cluster (leader or follower)
func (node *Node[State, F]) IsHealthy() bool {
	status := node.rawNode.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

func (node *Node[State, F]) Stop(ctx context.Context) error {
	node.logger.Infof("Stopping node")
	node.mu.Lock()
	isStarted := node.stopped != nil
	node.mu.Unlock()
	if !isStarted {
		node.logger.Infof("Node is not started, skipping stop")
		return nil
	}

	node.logger.Infof("Cancelling context...")
	node.cancel()

	node.logger.Infof("Waiting for node to stop...")
	select {
	case <-ctx.Done():
		node.logger.Infof("Context timed out while waiting for node to stop")
		return ctx.Err()
	case <-node.stopped:
		node.logger.Infof("Node stopped as expected")
	}

	node.logger.Infof("Stopping syncer...")
	node.syncer.stop()

	// todo: close channels

	return nil
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
