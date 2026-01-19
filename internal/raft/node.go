package raft

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
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

const (
	statusNormal = iota
	statusSyncing
	statusSnapshotting
	statusOutOfSync
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

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node struct {
	rawNode       *raft.RawNode
	logger        logging.Logger
	fsm           *FSM
	wal           WAL
	transport     Transport
	config        NodeConfig
	stopped       chan struct{}
	ctx           context.Context
	cancel        func()
	proposeCh     Queue[proposal]
	confState     *raftpb.ConfState
	futures       SyncMap[uint64, *future]
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

	spool                   Spool
	createSnapshotHistogram metric.Float64Histogram
	status                  *atomic.Int32
	snapshotThreshold       uint64
	compactionMargin        uint64
	taskExecutor            *singleTaskExecutor
	gatingTerminated        chan struct{}
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
	var initialConfState raftpb.ConfState
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

		initialConfState = raftpb.ConfState{
			Voters: voters,
		}
		if err := wal.CreateSnapshot(0, &initialConfState, data); err != nil {
			return nil, fmt.Errorf("creating initial snapshot: %w", err)
		}
	} else {
		if snapshot.Metadata.Index > 0 {
			logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
			if err := fsm.InstallSnapshot(context.Background(), snapshot); err != nil {
				panic(err)
			}

			logger.Infof("Snapshot restored successfully")
		} else {
			logger.Infof("Empty snapshot found, starting with empty FSM")
		}

		logger.Infof("Finished restoring FSM from storage")
		_, initialConfState, err = wal.InitialState()
		if err != nil {
			return nil, err
		}
	}

	initialStatus := atomic.Int32{}
	initialStatus.Store(statusNormal)

	node := &Node{
		logger:    logger,
		meter:     meter,
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
		// Syncer fields
		spool:             spool,
		snapshotThreshold: cfg.SnapshotThreshold,
		compactionMargin:  cfg.CompactionMargin,
		status:            &initialStatus,
		taskExecutor:      newSingleTaskExecutor(logger),
		confState:         &initialConfState,
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

	node.createSnapshotHistogram, err = meter.Float64Histogram("raft.syncer.create_snapshot.duration",
		metric.WithDescription("Time spent creating snapshot in syncer"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			// Fine-grained buckets for small values (0-100ms)
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			12, 15, 18, 20, 25, 30, 35, 40, 45, 50,
			60, 70, 80, 90, 100,
			// Medium buckets (100-500ms)
			125, 150, 175, 200, 250, 300, 350, 400, 450, 500,
			// Larger buckets (500ms-5s)
			600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 4000, 5000,
		),
	)
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

	isStoreUpToDate, err := node.fsm.IsStoreUpToDate(ctx)
	if err != nil {
		return fmt.Errorf("checking if store is up to date: %w", err)
	}

	if !isStoreUpToDate {
		node.logger.Infof("Store is not up to date, resuming from snapshot and tagging node as out of sync")
		node.status.Store(statusOutOfSync)
	} else {
		storeLastAppliedIndex, err := node.store.GetLastAppliedIndex()
		if err != nil {
			return err
		}

		if err := node.replaySpool(ctx, storeLastAppliedIndex); err != nil {
			return err
		}
	}

	processingTick := time.NewTicker(tickInterval / 20) // todo: make configurable
	defer processingTick.Stop()

	for {
		select {
		case <-ticker.C:
			// Prevent election timeouts from happening while syncing the FSM
			status := node.status.Load()
			if status != statusSyncing && status != statusOutOfSync {
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
				return err
			}
		case proposal := <-node.proposeCh.Recv():
			proposal.rejected <- node.rawNode.Propose(proposal.data)
		case <-node.gatingTerminated:
			if err := node.unspoolThenResume(ctx); err != nil {
				return err
			}
		case err := <-node.taskExecutor.error():
			return fmt.Errorf("task executor error: %w", err)
		case <-processingTick.C:
			if node.rawNode.HasReady() {
				now := time.Now()
				err := node.processReady(node.ctx)
				if err != nil {
					if !errors.Is(err, context.Canceled) {
						return err
					}
				}
				node.processEntryHistogram.Record(context.Background(), time.Since(now).Microseconds())
			}
		}
	}
}

func (node *Node) unspoolThenResume(ctx context.Context) error {
	node.logger.Infof("Background operation terminated, applying spooled entries before resuming...")
	node.gatingTerminated = nil
	node.status.Store(statusNormal)

	lastAppliedIndex, err := node.store.GetLastAppliedIndex()
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	node.logger.Infof("Unspooling from %d", lastAppliedIndex)
	if err := node.replaySpool(ctx, lastAppliedIndex); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	// todo: decorallate, this is not needed at this point
	if err := node.spool.Prune(lastAppliedIndex); err != nil {
		return fmt.Errorf("pruning spool: %w", err)
	}

	node.logger.Infof("Unspooling operation terminated, resuming...")

	return nil
}

func (node *Node) processReady(ctx context.Context) error {

	node.logger.Debugf("Processing ready")
	rd := node.rawNode.Ready()

	if rd.SoftState != nil {
		ss := rd.SoftState
		if ss.Lead != 0 && node.status.Load() == statusOutOfSync {
			if err := node.syncSnapshot(ctx, ss.Lead, rd.HardState.Commit); err != nil {
				return fmt.Errorf("syncing snapshot: %w", err)
			}
		}
		if node.lastSoftState != nil {
			status := node.rawNode.Status()
			logger := node.logger.WithFields(map[string]any{
				"lead": ss.Lead,
				"term": status.Term,
			})

			// leadership loss
			if node.lastSoftState.RaftState == raft.StateLeader && ss.RaftState != raft.StateLeader {
				logger.Infof("Leadership lost")
			}
			// acquire leadership
			if node.lastSoftState.RaftState != raft.StateLeader && ss.RaftState == raft.StateLeader {
				node.logger.Infof("Leadership gained")
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

		if err := node.fsm.InstallSnapshot(ctx, rd.Snapshot); err != nil {
			return fmt.Errorf("installing snapshot: %w", err)
		}

		// todo: since the snapshot is already written in storage at this point
		// we must be able to detect a crash and restart the restoration process
		// in case of rawNode recover
		if err := node.syncSnapshot(context.Background(), node.lastSoftState.Lead, rd.Snapshot.Metadata.Index); err != nil {
			return fmt.Errorf("restoring snapshot in storage: %w", err)
		}
	}

	// Send messages via transport
	node.logger.Debugf("Sending messages via transport")
	for _, msg := range rd.Messages {
		node.transport.Send(msg)
	}

	// Apply committed entries
	for _, entry := range rd.CommittedEntries {
		var cc raftpb.ConfChangeV2
		switch entry.Type {
		case raftpb.EntryConfChange:
			var ccV1 raftpb.ConfChange
			if err := ccV1.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("unmarshaling ConfChange: %w", err)
			}
			cc = ccV1.AsV2()
		case raftpb.EntryConfChangeV2:
			if err := cc.Unmarshal(entry.Data); err != nil {
				return fmt.Errorf("unmarshaling ConfChangeV2: %w", err)
			}
		default:
			continue
		}
		node.logger.
			WithFields(map[string]any{"transition": cc.Transition.String()}).
			Infof("Applying configuration change")
		node.confState = node.rawNode.ApplyConfChange(cc)
	}

	if len(rd.CommittedEntries) > 0 {
		switch node.status.Load() {
		case statusNormal:
			err := node.applyEntriesToFSM(ctx, node.confState, rd.CommittedEntries...)
			if err != nil {
				return fmt.Errorf("applying entries to FSM: %w", err)
			}
		default:
			node.logger.Debugf("Spool committed entries")
			err := node.spool.AppendCommittedEntries(ctx, rd.CommittedEntries...)
			if err != nil {
				return fmt.Errorf("spooling committed entries: %w", err)
			}
		}
	}

	// Advance the rawNode
	node.rawNode.Advance(rd)

	return nil
}

func (node *Node) applyEntriesAndResolveCommands(ctx context.Context, entries ...raftpb.Entry) error {
	start := time.Now()
	results, err := node.fsm.ApplyEntries(ctx, entries...)
	if err != nil {
		return fmt.Errorf("applying entries to FSM: %w", err)
	}
	node.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
	node.applyEntriesBatchSizeCounter.Add(ctx, int64(len(results)))
	node.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(results)))

	for _, result := range results {
		future, exists := node.futures.Load(result.CommandID)
		if !exists {
			continue
		}
		if !future.Done() {
			future.Resolve(result.Result, result.Error)
		}
	}

	return nil
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node) Apply(ctx context.Context, cmd *ledgerpb.Command) (any, error) {

	future := newFuture()

	node.futures.Store(cmd.Id, &future)
	defer node.futures.Delete(cmd.Id)

	cmdData, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	proposal := newProposal(cmdData)

	if !node.proposeCh.Push(proposal) {
		return nil, fmt.Errorf("propose channel full")
	}

	if err := proposal.wait(ctx); err != nil {
		return nil, err
	}

	return future.wait(ctx)
}

// NotifyApplied notifies the wrapper that a command with the given ID has been applied
// This should be called from the readyLoop when entries are applied
func (node *Node) NotifyApplied(commandID uint64, result any, err error) {
	future, exists := node.futures.Load(commandID)
	if !exists {
		return
	}

	if !future.Done() {
		future.Resolve(result, err)
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
	// todo: use to string of status.RaftState (work for cursor)
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
	ret, err := node.Apply(ctx, NewCreateLedgerCommand(cmd))
	if err != nil {
		return nil, fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	// ledgerInfo is already *ledgerpb.LedgerInfo
	ledgerInfo := ret.(*ledgerpb.LedgerInfo)
	return ledgerInfo, nil
}

func (node *Node) GetLedgerInfo(ctx context.Context, id uint32) (*ledgerpb.LedgerInfo, error) {
	return node.fsm.GetLedgerInfo(id)
}

// GetAllLedgersInfo returns all ledgers
func (node *Node) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	return node.fsm.GetAllLedgers(), nil
}

// DeleteLedger deletes a ledger via a FSM command
func (node *Node) DeleteLedger(ctx context.Context, id uint32) error {

	// Create the command
	cmd, err := NewDeleteLedgerCommand(id)
	if err != nil {
		return fmt.Errorf("creating delete ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, err = node.Apply(ctx, cmd)
	if err != nil {
		return fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	return nil
}

func (node *Node) CreateTransaction(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.CreateTransaction(ctx, ledgerID, parameters)
}

func (node *Node) RevertTransaction(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.RevertTransaction(ctx, ledgerID, parameters)
}

func (node *Node) SaveTransactionMetadata(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveTransactionMetadata(ctx, ledgerID, parameters)
}

func (node *Node) SaveAccountMetadata(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.SaveAccountMetadata(ctx, ledgerID, parameters)
}

func (node *Node) DeleteTransactionMetadata(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteTransactionMetadata(ctx, ledgerID, parameters)
}

func (node *Node) DeleteAccountMetadata(ctx context.Context, ledgerID uint32, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	return node.defaultLedger.DeleteAccountMetadata(ctx, ledgerID, parameters)
}

func (node *Node) Import(ctx context.Context, ledgerID uint32, stream chan *ledgerpb.Log) error {
	return node.defaultLedger.Import(ctx, ledgerID, stream)
}

func (node *Node) Export(ctx context.Context, ledgerID uint32, w service.ExportWriter) error {
	return node.defaultLedger.Export(ctx, ledgerID, w)
}

func (node *Node) GetAllLogs(ctx context.Context, ledgerID uint32, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	return node.store.GetAllLogs(ctx, ledgerID, from, to)
}

func (node *Node) CreateLog(ctx context.Context, ledgerID uint32, idempotency *ledgerpb.Idempotency, input *ledgerpb.CommandInput) (*ledgerpb.Log, error) {

	log, err := node.Apply(ctx, NewCreateLogCommand(input, ledgerID, idempotency))
	if err != nil {
		return nil, fmt.Errorf("applying insert log command via etcdraft: %w", err)
	}

	return log.(*ledgerpb.Log), nil
}

// syncSnapshot syncs a snapshot from a leader
func (node *Node) syncSnapshot(ctx context.Context, leader uint64, frozenAtIndex uint64) error {
	node.logger.
		WithFields(map[string]any{
			"leader": leader,
		}).
		Infof("Syncing snapshot from leader")

	node.status.Store(statusSyncing)

	node.runMaintenanceTask(ctx, frozenAtIndex, func(ctx context.Context) error {
		return node.fsm.SynchronizeWithLeader(ctx, leader)
	})

	return nil
}

func (node *Node) replaySpool(ctx context.Context, fromIndex uint64) error {

	node.logger.Infof("Replaying spool")

	until, err := node.spool.End()
	if err != nil {
		return fmt.Errorf("getting spool end position: %w", err)
	}

	batch := make([]raftpb.Entry, 0, 1000)
	if err := node.spool.ReplayUntil(ctx, *until, fromIndex, func(entry raftpb.Entry) error {
		batch = append(batch, entry)
		if len(batch) >= 1000 { // todo: configure
			if err := node.applyEntriesAndResolveCommands(ctx, batch...); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	}); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}
	if len(batch) > 0 {
		if err := node.applyEntriesAndResolveCommands(ctx, batch...); err != nil {
			return err
		}
	}

	return nil
}

func (node *Node) runMaintenanceTask(ctx context.Context, frozenAtIndex uint64, task func(ctx context.Context) error) {
	gatingTerminated := make(chan struct{})
	node.gatingTerminated = gatingTerminated

	node.taskExecutor.interrupt()
	node.taskExecutor.run(ctx, func(ctx context.Context) error {
		defer func() {
			close(gatingTerminated)
		}()

		if err := task(ctx); err != nil {
			return err
		}

		return node.replaySpool(ctx, frozenAtIndex)
	})
}

// applyEntriesToFSM applies entries directly to the FSM
func (node *Node) applyEntriesToFSM(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) error {
	err := node.applyEntriesAndResolveCommands(ctx, entries...)
	if err != nil {
		return err
	}

	lastSnapshot, err := node.wal.Snapshot()
	if err != nil {
		panic(fmt.Errorf("getting last snapshot: %w", err))
	}

	if entries[len(entries)-1].Index-lastSnapshot.Metadata.Index >= node.snapshotThreshold {
		// Short circuit the state machine
		// Futures entries will be spooled and applied later
		node.status.Store(statusSnapshotting)

		node.runMaintenanceTask(ctx, entries[len(entries)-1].Index, func(ctx context.Context) error {
			node.logger.WithFields(map[string]any{
				"applied":           entries[len(entries)-1].Index,
				"lastSnapshotIndex": lastSnapshot.Metadata.Index,
				"snapshotThreshold": node.snapshotThreshold,
				"compactionMargin":  node.compactionMargin,
			}).Infof("Creating new snapshot")

			startTime := time.Now()
			data, err := node.fsm.CreateSnapshot(ctx)
			if err != nil {
				return err
			}

			err = node.wal.CreateSnapshot(entries[len(entries)-1].Index, confState, data)
			if err != nil {
				return err
			}
			duration := time.Since(startTime)
			node.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))

			// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
			// todo: decorallate compaction as it increase the spooling time and this is not needed
			if entries[len(entries)-1].Index > node.compactionMargin {
				err = node.wal.Compact(entries[len(entries)-1].Index - node.compactionMargin)
				if err != nil {
					return err
				}
			}

			return nil
		})
	}

	return nil
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

	// todo: close channels

	return nil
}

func (node *Node) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return node.fsm.GetLedgerByName(name)
}

type proposal struct {
	data     []byte
	rejected chan error
}

func (p proposal) wait(ctx context.Context) error {
	select {
	case err := <-p.rejected:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func newProposal(data []byte) proposal {
	return proposal{
		data:     data,
		rejected: make(chan error, 1),
	}
}
