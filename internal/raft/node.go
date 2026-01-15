package raft

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/otlplogs"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node struct {
	rawNode       *raft.RawNode
	logger        logging.Logger
	mu            sync.RWMutex
	syncer        *syncer
	fsm           *defaultFSM
	wal           *WAL
	transport     *GRPCTransport
	config        NodeConfig
	stopped       chan struct{}
	ctx           context.Context
	cancel        func()
	proposeCh     Queue[[]byte]
	confState     *raftpb.ConfState
	futures       map[uint64]*applyFuture // Map of command ID -> future
	lastSoftState *raft.SoftState

	meter                          metric.Meter
	applyEntriesHistogram          metric.Int64Histogram
	applyEntriesBatchSizeCounter   metric.Int64Counter
	applyEntriesBatchSizeHistogram metric.Int64Histogram
	processEntryHistogram          metric.Int64Histogram
	appendEntriesHistogram         metric.Int64Histogram
	leadMonitorHistogram           metric.Int64Gauge
	defaultLedger                  *service.DefaultController
	runtimeStore                   store.Runtime
}

// NewNode creates a new wrapper around a RawNode
func NewNode(
	cfg NodeConfig,
	transport *GRPCTransport,
	runtimeStore store.Runtime,
	logger logging.Logger,
	meter metric.Meter,
) (*Node, error) {

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
	if cfg.ProposeQueueCapacity == 0 {
		cfg.ProposeQueueCapacity = 100
	}

	if err := os.MkdirAll(cfg.WalDir, 0755); err != nil {
		return nil, fmt.Errorf("creating wal directory: %w", err)
	}

	wal, err := NewWAL(filepath.Join(cfg.WalDir, "wal"), logger)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	spool, err := newFileSpool(filepath.Join(cfg.WalDir, "spool"))
	if err != nil {
		return nil, fmt.Errorf("creating spool: %w", err)
	}

	fsm := newFSM(logger, runtimeStore, transport)

	snapshot, err := wal.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}
	if len(snapshot.Metadata.ConfState.Voters) == 0 {
		logger.Infof("Detected empty WAL, creating initial snapshot")
		voters := make([]uint64, 0, len(cfg.Peers)+1)
		voters = append(voters, cfg.NodeID)

		for _, peerEntry := range cfg.Peers {
			voters = append(voters, peerEntry.ID)
		}

		data, err := fsm.CreateSnapshot(context.Background())
		if err != nil {
			return nil, fmt.Errorf("creating initial snapshot data: %w", err)
		}

		if err := wal.CreateSnapshot(0, &raftpb.ConfState{
			Voters: voters,
		}, data); err != nil {
			return nil, fmt.Errorf("creating initial snapshot: %w", err)
		}
	} else {
		if snapshot.Metadata.Index > 0 {
			logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
			if err := fsm.RestoreSnapshot(snapshot); err != nil {
				panic(err)
			}
			logger.Infof("Snapshot restored successfully")
		} else {
			logger.Infof("No snapshot found, starting with empty FSM")
		}

		logger.Infof("Finished restoring FSM from storage")
	}

	node := &Node{
		logger:  logger,
		meter:   meter,
		futures: make(map[uint64]*applyFuture),
		syncer: newSyncer(
			spool,
			fsm,
			logger,
			wal,
			meter,
			cfg.SnapshotThreshold,
			cfg.CompactionMargin,
		),
		wal:       wal,
		transport: transport,
		config:    cfg,
		proposeCh: NewQueueObserver[[]byte](
			"raft.node.propose",
			NewSimpleQueue[[]byte](cfg.ProposeQueueCapacity),
			WithLogger[[]byte](logger),
			WithMeter[[]byte](meter),
		),
		runtimeStore: runtimeStore,
		fsm:          fsm,
	}
	go node.syncer.run()

	node.defaultLedger = service.NewDefaultController(node, runtimeStore, logger)

	for _, peerEntry := range cfg.Peers {
		logger := logger.WithFields(map[string]any{"peer": peerEntry})
		logger.Debugf("Adding peer to transport")
		transport.AddPeer(peerEntry.ID, peerEntry.Address)
	}

	node.applyEntriesHistogram, err = meter.Int64Histogram("raft.apply_entries.duration",
		metric.WithDescription("Time spent applying entries to FSM"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			// Fine-grained buckets for small values (0-100ms)
			0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
			12000, 15000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000,
			60000, 70000, 80000, 90000, 100000,
			// Medium buckets (100-500ms)
			125000, 150000, 175000, 200000, 250000, 300000, 350000, 400000, 450000, 500000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.applyEntriesBatchSizeCounter, err = meter.Int64Counter("raft.apply_entries.batch_size",
		metric.WithDescription("Size of batches passed to ApplyEntries"),
		metric.WithUnit("1"),
	)
	if err != nil {
		panic(err)
	}

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

	node.appendEntriesHistogram, err = meter.Int64Histogram("raft.append_entries",
		metric.WithDescription("Time spending appending entries to wal"),
		metric.WithUnit("us"),
	)
	if err != nil {
		panic(err)
	}

	node.processEntryHistogram, err = meter.Int64Histogram("raft.process_entry",
		metric.WithDescription("Time spent processing ready from raft"),
		metric.WithUnit("us"),
	)
	if err != nil {
		panic(err)
	}

	node.leadMonitorHistogram, err = meter.Int64Gauge("raft.node.lead")
	if err != nil {
		panic(err)
	}

	return node, nil
}

func (node *Node) Start(ctx context.Context) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	node.stopped = make(chan struct{})
	node.ctx, node.cancel = context.WithCancel(context.Background())

	raftConfig := &raft.Config{
		ID: node.config.NodeID,
		// todo: add random delay on election tick
		ElectionTick:              node.config.ElectionTick,
		HeartbeatTick:             node.config.HeartbeatTick,
		Storage:                   node.wal,
		MaxSizePerMsg:             node.config.MaxSizePerMsg,
		MaxInflightMsgs:           node.config.MaxInflightMsgs,
		Logger:                    NewLoggerAdapter(node.logger),
		DisableProposalForwarding: true,
	}

	node.logger.WithFields(map[string]any{
		"config": raftConfig,
	}).Infof("Starting raft node")

	var err error
	node.rawNode, err = raft.NewRawNode(raftConfig)
	if err != nil {
		return fmt.Errorf("creating raw rawNode: %w", err)
	}

	go node.readyLoop()

	return nil
}

// readyLoop processes Ready structures from etcd/raft for this bucket group with a specific message channel
func (node *Node) readyLoop() {

	defer otlplogs.RecoverAndLogPanics(node.logger)

	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	defer func() {
		_ = node.wal.Close()
	}()

	_, initialConfState, err := node.wal.InitialState()
	if err != nil {
		panic(err)
	}
	node.confState = &initialConfState

	processingTick := time.NewTicker(tickInterval / 20) // todo: make configurable
	defer processingTick.Stop()

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
				now := time.Now()
				err := node.processReady(node.ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						panic(err)
					}
				}
				node.processEntryHistogram.Record(context.Background(), time.Since(now).Microseconds())
			}
		}
	}
}

func (node *Node) processReady(ctx context.Context) error {

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
		node.leadMonitorHistogram.Record(ctx, int64(ss.Lead))
		node.lastSoftState = ss
	}

	if len(rd.Entries) > 0 {
		now := time.Now()
		if err := node.wal.Append(rd.HardState, rd.Entries); err != nil {
			return fmt.Errorf("appending entries to storage: %w", err)
		}
		node.appendEntriesHistogram.Record(ctx, time.Since(now).Microseconds())
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		node.logger.
			WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).
			Infof("Applying snapshot sent by leader")

		if err := node.wal.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("applying snapshot to storage: %w", err)
		}

		node.rawNode.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)

		// todo: since the snapshot is already written in storage at this point
		// we must be able to detect a crash and restart the restoration process
		// in case of rawNode recover
		if err := node.syncer.SyncSnapshot(context.Background(), node.lastSoftState.Lead, rd.Snapshot); err != nil {
			panic(fmt.Errorf("restoring snapshot in storage: %w", err))
		}
	}

	// Send messages via transport
	for _, msg := range rd.Messages {
		node.transport.Send(msg)
	}

	// Apply committed entries
	var (
		results        []ApplyResult
		commands       = make([]*ledgerpb.Command, 0, len(rd.CommittedEntries))
		commandIndexes = make([]uint64, 0, len(rd.CommittedEntries))
	)
	for _, entry := range rd.CommittedEntries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				continue
			}

			// Decode the command to get its ID
			var cmd ledgerpb.Command
			if err := proto.Unmarshal(entry.Data, &cmd); err != nil {
				return fmt.Errorf("unmarshaling command: %w", err)
			}

			commands = append(commands, &cmd)
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
	var (
		err error
	)
	if len(commands) > 0 {
		confState := *node.confState
		start := time.Now()
		results, err = node.syncer.ApplyEntries(
			node.ctx,
			rd.CommittedEntries[len(rd.CommittedEntries)-1].Index,
			&confState,
			commands...,
		)
		if err != nil {
			return fmt.Errorf("applying entries to FSM: %w", err)
		}

		node.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
		node.applyEntriesBatchSizeCounter.Add(ctx, int64(len(commands)))
		node.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(commands)))
	}

	for i, result := range results {
		node.NotifyApplied(commands[i].Id, result.Result, commandIndexes[i], result.Error)
		if result.Error != nil {
			node.logger.
				WithFields(map[string]any{
					"error":     result.Error,
					"index":     commandIndexes[i],
					"commandID": commands[i].Id,
				}).
				Errorf("Failed to apply entry to bucket FSM")
		}
	}

	// Advance the rawNode
	node.rawNode.Advance(rd)

	return nil
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node) Apply(cmd *ledgerpb.Command, timeout time.Duration) (uint64, any, error) {

	// Create a future for this application using command ID as key
	future := &applyFuture{
		index: 0, // Will be set when entry is applied
		ch:    make(chan error, 1),
	}

	// Register the future using command ID
	node.mu.Lock()
	node.futures[cmd.Id] = future
	node.mu.Unlock()

	cmdData, err := proto.Marshal(cmd)
	if err != nil {
		return 0, nil, fmt.Errorf("marshaling command: %w", err)
	}

	// Wait for the future to complete with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Propose the command
	if !node.proposeCh.Push(cmdData) {
		return 0, nil, fmt.Errorf("propose channel full")
	}

	select {
	case err := <-future.ch:
		node.mu.Lock()
		delete(node.futures, cmd.Id)
		node.mu.Unlock()
		if err != nil {
			return 0, nil, err
		}
		return future.index, future.result, nil
	case <-ctx.Done():
		// Timeout - clean up the future
		node.mu.Lock()
		delete(node.futures, cmd.Id)
		node.mu.Unlock()
		return 0, nil, ctx.Err()
	}
}

// NotifyApplied notifies the wrapper that a command with the given ID has been applied
// This should be called from the readyLoop when entries are applied
func (node *Node) NotifyApplied(commandID uint64, result any, index uint64, err error) {
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

// Status returns the current status of the rawNode
func (node *Node) Status() raft.Status {
	return node.rawNode.Status()
}

func (node *Node) IsLeader() bool {
	status := node.rawNode.Status()
	return status.Lead == status.ID
}

func (node *Node) GetLeader() uint64 {
	return node.rawNode.Status().Lead
}

// GetClusterState returns the current state of the Raft cluster
func (node *Node) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState, error) {
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
	hardState, _, err := node.wal.InitialState()
	if err != nil {
		return nil, fmt.Errorf("getting initial state: %w", err)
	}

	// Get last index from storage
	lastIndex, err := node.wal.LastIndex()
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

	return &ledgerpb.ClusterState{
		State:      stateStr,
		Leader:     uint(leaderID),
		Nodes:      nodes,
		LocalNode:  uint(node.config.NodeID),
		RaftStatus: raftStatus,
		InnerState: node.fsm.GetState(),
	}, nil
}

// IsHealthy returns true if the rawNode is connected to the cluster (leader or follower)
func (node *Node) IsHealthy() bool {
	status := node.rawNode.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

// CreateLedger creates a new ledger via a FSM command
func (node *Node) CreateLedger(ctx context.Context, cmd *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {

	// Apply the command via Raft (waits for application)
	_, ret, err := node.Apply(NewCreateLedgerCommand(cmd), 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	// ledgerInfo is already *ledgerpb.LedgerInfo
	ledgerInfo := ret.(*ledgerpb.LedgerInfo)
	return ledgerInfo, nil
}

func (node *Node) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return node.fsm.GetLedger(name)
}

// GetAllLedgersInfo returns all ledgers
func (node *Node) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	return node.fsm.GetAllLedgers(), nil
}

// DeleteLedger deletes a ledger via a FSM command
func (node *Node) DeleteLedger(ctx context.Context, name string) error {
	// Create the command
	cmd, err := NewDeleteLedgerCommand(name)
	if err != nil {
		return fmt.Errorf("creating delete ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, _, err = node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	node.logger.WithFields(map[string]any{"name": name, "commandID": cmd.Id}).Infof("Ledger deleted via Raft")
	return nil
}

func (node *Node) CreateTransaction(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.CreateTransaction(ctx, ledger, parameters)
}

func (node *Node) RevertTransaction(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.RevertTransaction(ctx, ledger, parameters)
}

func (node *Node) SaveTransactionMetadata(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveTransactionMetadata(ctx, ledger, parameters)
}

func (node *Node) SaveAccountMetadata(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveAccountMetadata(ctx, ledger, parameters)
}

func (node *Node) DeleteTransactionMetadata(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteTransactionMetadata(ctx, ledger, parameters)
}

func (node *Node) DeleteAccountMetadata(ctx context.Context, ledger string, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteAccountMetadata(ctx, ledger, parameters)
}

func (node *Node) Import(ctx context.Context, ledger string, stream chan *ledgerpb.Log) error {
	return node.defaultLedger.Import(ctx, ledger, stream)
}

func (node *Node) Export(ctx context.Context, ledger string, w service.ExportWriter) error {
	return node.defaultLedger.Export(ctx, ledger, w)
}

func (node *Node) GetAllLogs(ctx context.Context, ledger string, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	return node.runtimeStore.GetAllLogs(ctx, ledger, from, to)
}

func (node *Node) CreateLog(ctx context.Context, ledger string, idempotency *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {

	// Create a command to insert the log
	cmd, err := NewCreateLogCommand(input, ledger, idempotency)
	if err != nil {
		return nil, fmt.Errorf("creating insert log command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, log, err := node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying insert log command via etcdraft: %w", err)
	}

	node.logger.
		WithFields(map[string]any{"commandID": cmd.Id}).
		Debugf("Log inserted via ledger Raft")

	return log.(*ledgerpb.Log), nil
}

func (node *Node) Stop(ctx context.Context) error {
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
