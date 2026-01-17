package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source node.go -destination node_generated_test.go -typed -package raft . WAL
type WAL interface {
	raft.Storage
	CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error
	Compact(u uint64) error
	Append(state raftpb.HardState, entries []raftpb.Entry) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source node.go -destination node_generated_test.go -typed -package raft . Transport
type Transport interface {
	GetPeerConnection(leader uint64) *grpc.ClientConn
	Unreachable() <-chan uint64
	Recv() <-chan raftpb.Message
	Send(msg raftpb.Message)
}

type proposal struct {
	data     []byte
	rejected chan error
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node struct {
	rawNode       *raft.RawNode
	logger        logging.Logger
	syncer        *syncer
	fsm           *defaultFSM
	wal           WAL
	transport     Transport
	config        NodeConfig
	stopped       chan struct{}
	ctx           context.Context
	cancel        func()
	proposeCh     Queue[proposal]
	confState     *raftpb.ConfState
	futures       SyncMap[uint64, *applyFuture]
	lastSoftState *raft.SoftState

	meter                          metric.Meter
	applyEntriesHistogram          metric.Int64Histogram
	applyEntriesBatchSizeCounter   metric.Int64Counter
	applyEntriesBatchSizeHistogram metric.Int64Histogram
	processEntryHistogram          metric.Int64Histogram
	appendEntriesHistogram         metric.Int64Histogram
	leadMonitorHistogram           metric.Int64Gauge
	defaultLedger                  *service.DefaultController
	store                          store.Store
}

// NewNode creates a new wrapper around a RawNode
func NewNode(
	cfg NodeConfig,
	transport Transport,
	store store.Store,
	logger logging.Logger,
	meter metric.Meter,
	spool Spool,
	wal WAL,
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

	fsm, _ := newFSM(logger, store, transport)

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
			logger.Infof("Empty snapshot found, starting with empty FSM")
		}

		logger.Infof("Finished restoring FSM from storage")
	}

	node := &Node{
		logger: logger,
		meter:  meter,
		syncer: newSyncer(
			spool,
			fsm,
			logger,
			wal,
			meter,
			store,
			cfg.SnapshotThreshold,
			cfg.CompactionMargin,
		),
		wal:       wal,
		transport: transport,
		config:    cfg,
		proposeCh: NewQueueObserver[proposal](
			"raft.node.propose",
			NewSimpleQueue[proposal](cfg.ProposeQueueCapacity),
			WithLogger[proposal](logger),
			WithMeter[proposal](meter),
		),
		store: store,
		fsm:   fsm,
	}

	node.defaultLedger = service.NewDefaultController(node, store, logger)

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

	node.stopped = make(chan struct{})
	node.ctx, node.cancel = context.WithCancel(ctx)

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

	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	_, initialConfState, err := node.wal.InitialState()
	if err != nil {
		panic(err)
	}
	node.confState = &initialConfState

	if err := node.syncer.Replay(ctx); err != nil {
		return fmt.Errorf("replaying storage: %w", err)
	}

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
			return nil
		case nodeID := <-node.transport.Unreachable():
			node.logger.Errorf("Node %x is unreachable", nodeID)
			node.rawNode.ReportUnreachable(nodeID)
		case msg := <-node.transport.Recv():
			if err := node.rawNode.Step(msg); err != nil {
				panic(err)
			}
		case proposal := <-node.proposeCh.Recv():
			proposal.rejected <- node.rawNode.Propose(proposal.data)
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

	return nil
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

	now := time.Now()
	if err := node.wal.Append(rd.HardState, rd.Entries); err != nil {
		return fmt.Errorf("appending entries to storage: %w", err)
	}
	node.appendEntriesHistogram.Record(ctx, time.Since(now).Microseconds())

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
	for _, entry := range rd.CommittedEntries {
		switch entry.Type {
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
	if len(rd.CommittedEntries) > 0 {
		confState := *node.confState
		start := time.Now()
		results, err := node.syncer.ApplyEntries(
			node.ctx,
			&confState,
			rd.CommittedEntries...,
		)
		if err != nil {
			return fmt.Errorf("applying entries to FSM: %w", err)
		}

		node.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
		node.applyEntriesBatchSizeCounter.Add(ctx, int64(len(results)))
		node.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(results)))

		for _, result := range results {
			node.NotifyApplied(result.CommandID, result.Result, result.Error)
			if result.Error != nil {
				panic(result.Error)
			}
		}
	}

	// Advance the rawNode
	node.rawNode.Advance(rd)

	return nil
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node) Apply(cmd *ledgerpb.Command, timeout time.Duration) (any, error) {

	future := &applyFuture{
		ch: make(chan error, 1),
	}

	node.futures.Store(cmd.Id, future)
	defer node.futures.Delete(cmd.Id)

	cmdData, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	proposal := proposal{
		data:     cmdData,
		rejected: make(chan error, 1),
	}

	if !node.proposeCh.Push(proposal) {
		return nil, fmt.Errorf("propose channel full")
	}

	select {
	case err := <-proposal.rejected:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case err := <-future.ch:
		if err != nil {
			return nil, err
		}
		return future.result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NotifyApplied notifies the wrapper that a command with the given ID has been applied
// This should be called from the readyLoop when entries are applied
func (node *Node) NotifyApplied(commandID uint64, result any, err error) {
	future, exists := node.futures.Load(commandID)
	if !exists {
		return
	}

	if !future.done {
		future.done = true
		future.result = result
		future.err = err
		// Send error (or nil) to channel
		select {
		case future.ch <- err:
		default:
			// Channel already closed or error already sent
		}
	}
}

func (node *Node) IsLeader() bool {
	if node.rawNode == nil {
		return false
	}
	status := node.rawNode.Status()
	return status.Lead == status.ID
}

func (node *Node) GetLeader() uint64 {
	if node.rawNode == nil {
		return 0
	}
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
	ret, err := node.Apply(NewCreateLedgerCommand(cmd), 10*time.Second)
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

	// todo: optimist check but there can be concurrent requests
	if _, err := node.fsm.GetLedger(name); err != nil {
		return fmt.Errorf("ledger '%s' not found: %w", name, err)
	}

	// Create the command
	cmd, err := NewDeleteLedgerCommand(name)
	if err != nil {
		return fmt.Errorf("creating delete ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, err = node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

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
	return node.store.GetAllLogs(ctx, ledger, from, to)
}

func (node *Node) CreateLog(ctx context.Context, ledger string, idempotency *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {

	cmd := NewCreateLogCommand(input, ledger, idempotency)

	log, err := node.Apply(cmd, 5*time.Second) // todo: make timeouts configurable
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
	isStarted := node.stopped != nil
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

	// todo: close channels

	return nil
}

// applyFuture represents a future for an applied entry
type applyFuture struct {
	ch     chan error
	result any
	done   bool
	err    error
}
