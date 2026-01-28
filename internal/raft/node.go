package raft

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger-v3-poc/internal/proto/ledgerpb"
	raftcommand "github.com/formancehq/ledger-v3-poc/internal/proto/raftpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

const (
	statusNormal = iota
	statusSyncing
	statusSnapshotting
	statusOutOfSync
)

type LogStreamer interface {
	store.LogStreamer
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source node.go -destination node_generated_test.go -typed -package raft . WAL
type WAL interface {
	raft.Storage
	CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error
	Compact(u uint64) error
	Append(state raftpb.HardState, entries []raftpb.Entry) error
	ApplySnapshot(snapshot raftpb.Snapshot) error
	Close() error
}

type LogStreamerProvider interface {
	GetForPeer(id uint64) (LogStreamer, error)
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node struct {
	rawNode             *raft.RawNode
	logger              logging.Logger
	fsm                 *FSM
	wal                 WAL
	transport           Transport
	config              NodeConfig
	proposeCh           chan *proposal
	confState           *raftpb.ConfState
	futures             SyncMap[uint64, *future]
	lastSoftState       atomic.Pointer[raft.SoftState]
	logStreamerProvider LogStreamerProvider

	store             store.Store
	spool             Spool
	status            *atomic.Int32
	snapshotThreshold uint64
	compactionMargin  uint64
	taskExecutor      *singleTaskExecutor
	gatingTerminated  chan struct{}
	readies           chan raft.Ready
	readyTerminated   chan raft.Ready
	tasks             *taskSet
	stopChannel       chan chan struct{}

	// Metrics
	meter                             metric.Meter
	applyEntriesHistogram             metric.Int64Histogram
	applyEntriesBatchSizeCounter      metric.Int64Counter
	applyEntriesBatchSizeHistogram    metric.Int64Histogram
	processEntryHistogram             metric.Int64Histogram
	appendEntriesHistogram            metric.Int64Histogram
	leadMonitorHistogram              metric.Int64Gauge
	committedEntriesPerReadyHistogram metric.Int64Histogram
	createSnapshotHistogram           metric.Float64Histogram
	proposeQueueLoadHistogram         metric.Int64Histogram
	proposeQueueFullCounter           metric.Float64Counter
	proposeQueueInflight              atomic.Int32
	readyWaitDurationHistogram        metric.Int64Histogram
	commandDurationHistogram          metric.Int64Histogram
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
	logStreamerProvider LogStreamerProvider,
) (*Node, error) {

	cfg.SetDefaults()

	fsm, err := newFSM(logger, store, transport)
	if err != nil {
		return nil, fmt.Errorf("creating FSM: %w", err)
	}

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
		logger:              logger,
		meter:               meter,
		wal:                 wal,
		transport:           transport,
		config:              cfg,
		proposeCh:           make(chan *proposal, cfg.ProposeQueueCapacity),
		store:               store,
		fsm:                 fsm,
		spool:               spool,
		snapshotThreshold:   cfg.SnapshotThreshold,
		compactionMargin:    cfg.CompactionMargin,
		status:              &initialStatus,
		taskExecutor:        newSingleTaskExecutor(logger),
		confState:           &initialConfState,
		logStreamerProvider: logStreamerProvider,
		readies:             make(chan raft.Ready, 1),
		readyTerminated:     make(chan raft.Ready, 1),
		tasks:               newTaskSet(),
		stopChannel:         make(chan chan struct{}),
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
			1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, 2000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.appendEntriesHistogram, err = meter.Int64Histogram("raft.append_entries",
		metric.WithDescription("Time spending appending entries to wal"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 200, 400, 700, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 10000, 50000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.processEntryHistogram, err = meter.Int64Histogram("raft.process_entry",
		metric.WithDescription("Time spent processing ready from raft"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 200, 400, 700, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 10000, 50000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.committedEntriesPerReadyHistogram, err = meter.Int64Histogram("raft.ready.committed_entries",
		metric.WithDescription("Number of committed entries per Ready"),
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			0, 1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, 2000,
		),
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

	node.proposeQueueFullCounter, err = meter.Float64Counter("raft.node.propose.full", metric.WithUnit("1"))
	if err != nil {
		panic(err)
	}

	node.proposeQueueLoadHistogram, err = meter.Int64Histogram(
		"raft.node.propose.load",
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			expBoundaries(12, cfg.ProposeQueueCapacity)...,
		),
	)
	if err != nil {
		panic(err)
	}

	node.readyWaitDurationHistogram, err = meter.Int64Histogram(
		"raft.node.ready.wait_duration",
		metric.WithDescription("Time spent waiting for a Ready from Raft"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.commandDurationHistogram, err = meter.Int64Histogram(
		"raft.command.duration",
		metric.WithDescription("Total time to resolve a command (from Apply call to future resolution)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000, 2000000, 5000000,
		),
	)
	if err != nil {
		panic(err)
	}

	return node, nil
}

func (node *Node) Run(ctx context.Context) error {

	raftConfig := &raft.Config{
		ID:                        node.config.NodeID,
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

	node.tasks.add(newTask(node.orchestrate))
	node.tasks.add(newTask(node.processReadies))
	node.tasks.run(ctx)

	select {
	case ch := <-node.stopChannel:
		if err := node.tasks.stop(); err != nil {
			node.logger.Errorf("Error stopping task pool: %v", err)
		}
		close(ch)
		return nil
	case err := <-node.tasks.err():
		return fmt.Errorf("task pool error: %w", err)
	}
}

func (node *Node) processReadies(ctx context.Context, stop chan struct{}) error {
	for {
		waitStart := time.Now()
		select {
		case rd := <-node.readies:
			node.readyWaitDurationHistogram.Record(context.Background(), time.Since(waitStart).Microseconds())
			now := time.Now()
			err := node.processReady(ctx, rd)
			node.processEntryHistogram.Record(context.Background(), time.Since(now).Microseconds())
			if err != nil {
				return err
			}
			select {
			case node.readyTerminated <- rd:
			case <-stop:
				return nil
			}
		case <-stop:
			return nil
		}
	}
}

func (node *Node) processReady(ctx context.Context, rd raft.Ready) error {

	node.logger.Debugf("Processing ready")

	node.committedEntriesPerReadyHistogram.Record(context.Background(), int64(len(rd.CommittedEntries)))

	if rd.SoftState != nil {
		ss := rd.SoftState
		if ss.Lead != 0 && node.status.Load() == statusOutOfSync {
			if err := node.syncSnapshot(ctx, ss.Lead); err != nil {
				return fmt.Errorf("syncing snapshot: %w", err)
			}
		}

		actualNodeLastSoftState := node.lastSoftState.Load()
		if actualNodeLastSoftState != nil && *actualNodeLastSoftState != *ss {
			status := node.rawNode.Status()
			logger := node.logger.WithFields(map[string]any{
				"lead": ss.Lead,
				"term": status.Term,
			})

			// leadership loss
			if actualNodeLastSoftState.RaftState == raft.StateLeader && ss.RaftState != raft.StateLeader {
				logger.Infof("Leadership lost")
			}
			// acquire leadership
			if actualNodeLastSoftState.RaftState != raft.StateLeader && ss.RaftState == raft.StateLeader {
				node.logger.Infof("Leadership gained")
			}
		}
		node.leadMonitorHistogram.Record(ctx, int64(ss.Lead))

		node.lastSoftState.Store(ss)
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

		if err := node.fsm.InstallSnapshot(ctx, rd.Snapshot); err != nil {
			return fmt.Errorf("installing snapshot: %w", err)
		}

		node.rawNode.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)

		// todo: since the snapshot is already written in storage at this point
		// we must be able to detect a crash and restart the restoration process
		// in case of rawNode recover
		if err := node.syncSnapshot(ctx, node.lastSoftState.Load().Lead); err != nil {
			return fmt.Errorf("restoring snapshot in storage: %w", err)
		}
	}

	// Send messages via transport
	node.logger.Debugf("Sending messages via transport")
	node.transport.Send(rd.Messages)

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
		future.Resolve(result.Result, result.Error)
	}

	return nil
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

		node.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
			node.logger.WithFields(map[string]any{
				"applied":           entries[len(entries)-1].Index,
				"lastSnapshotIndex": lastSnapshot.Metadata.Index,
				"snapshotThreshold": node.snapshotThreshold,
				"compactionMargin":  node.compactionMargin,
			}).Infof("Creating new snapshot")

			startTime := time.Now()
			data, err := node.fsm.CreateSnapshot(ctx)
			if err != nil {
				return 0, err
			}

			err = node.wal.CreateSnapshot(entries[len(entries)-1].Index, confState, data)
			if err != nil {
				return 0, err
			}
			duration := time.Since(startTime)
			node.createSnapshotHistogram.Record(ctx, float64(duration.Milliseconds()))

			// todo: Each follower should have a "matchIndex", we can use it to determine the index to compact
			// todo: decorallate compaction as it increase the spooling time and this is not needed
			if entries[len(entries)-1].Index > node.compactionMargin {
				err = node.wal.Compact(entries[len(entries)-1].Index - node.compactionMargin)
				if err != nil {
					return 0, err
				}
			}

			return entries[len(entries)-1].Index, nil
		})
	}

	return nil
}

func (node *Node) orchestrate(ctx context.Context, stop chan struct{}) error {

	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	processingTick := time.NewTicker(tickInterval / 10) // todo: make configurable
	defer processingTick.Stop()

	// Helper to process a batch of messages
	stepMessages := func(msgs []raftpb.Message) error {
		for _, msg := range msgs {
			if err := node.rawNode.Step(msg); err != nil {
				return err
			}
		}
		return nil
	}

	for {
		select {
		case <-ticker.C:
			// Prevent election timeouts from happening while syncing the FSM
			status := node.status.Load()
			if status != statusSyncing && status != statusOutOfSync {
				node.rawNode.Tick()
			}
		case msgs := <-node.transport.RecvHighPriority():
			if err := stepMessages(msgs); err != nil {
				return err
			}
		default:
			select {
			case msgs := <-node.transport.RecvHighPriority():
				if err := stepMessages(msgs); err != nil {
					return err
				}
			case msgs := <-node.transport.RecvMediumPriority():
				if err := stepMessages(msgs); err != nil {
					return err
				}
			case p := <-node.proposeCh:
				node.proposeQueueInflight.Add(-1)
				p.Resolve(nil, node.rawNode.Propose(p.data))
			default:
				select {
				case <-node.gatingTerminated:
					if err := node.unspoolAndResume(ctx); err != nil {
						return err
					}
				case rd := <-node.readyTerminated:
					node.rawNode.Advance(rd)
					if node.rawNode.HasReady() {
						node.readies <- node.rawNode.Ready()
					} else {
						processingTick.Reset(tickInterval / 10)
						node.readyTerminated = nil
					}
				case <-processingTick.C:
					if !node.rawNode.HasReady() {
						continue
					}

					node.readyTerminated = make(chan raft.Ready, 1)
					processingTick.Stop()
					node.readies <- node.rawNode.Ready()
				case <-stop:
					node.logger.Infof("Stopping readyLoop as context was cancelled")
					node.taskExecutor.interrupt()
					return nil
				case nodeID := <-node.transport.Unreachable():
					node.rawNode.ReportUnreachable(nodeID)
				case msgs := <-node.transport.RecvHighPriority():
					if err := stepMessages(msgs); err != nil {
						return err
					}
				case msgs := <-node.transport.RecvMediumPriority():
					if err := stepMessages(msgs); err != nil {
						return err
					}
				case msgs := <-node.transport.RecvLowPriority():
					if err := stepMessages(msgs); err != nil {
						return err
					}
				case p := <-node.proposeCh:
					node.proposeQueueInflight.Add(-1)
					p.Resolve(nil, node.rawNode.Propose(p.data))
				case err := <-node.taskExecutor.error():
					return fmt.Errorf("task executor error: %w", err)
				}
			}
		}
	}
}

func (node *Node) unspoolAndResume(ctx context.Context) error {
	node.logger.Infof("Background operation terminated, applying spooled entries before resuming...")

	lastAppliedIndex, err := node.store.GetLastAppliedIndex()
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	if err := node.replaySpool(ctx, lastAppliedIndex); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	node.status.Store(statusNormal)

	// todo: measure time
	if err := node.spool.Prune(lastAppliedIndex); err != nil {
		return fmt.Errorf("pruning spool: %w", err)
	}

	node.logger.Infof("Unspooling operation terminated, resuming...")
	node.gatingTerminated = nil

	return nil
}

// syncSnapshot syncs a snapshot from a leader
func (node *Node) syncSnapshot(ctx context.Context, leader uint64) error {
	node.logger.
		WithFields(map[string]any{
			"leader": leader,
		}).
		Infof("Syncing snapshot from leader")

	node.status.Store(statusSyncing)

	node.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		logStreamer, err := node.logStreamerProvider.GetForPeer(leader)
		if err != nil {
			return 0, fmt.Errorf("getting log reader for leader %d: %w", leader, err)
		}
		return node.fsm.SynchronizeWithLeader(ctx, logStreamer)
	})

	return nil
}

func (node *Node) replaySpool(ctx context.Context, fromIndex uint64) error {

	node.logger.Infof("Replaying spool")

	until, err := node.spool.End()
	if err != nil {
		return fmt.Errorf("getting spool end position: %w", err)
	}

	count := 0
	batch := make([]raftpb.Entry, 0, 1000)
	logFields := map[string]any{}
	var lastEntry *raftpb.Entry
	if err := node.spool.ReplayUntil(ctx, *until, fromIndex, func(entry raftpb.Entry) error {
		batch = append(batch, entry)
		if len(batch) >= 1000 { // todo: configure
			if err := node.applyEntriesAndResolveCommands(ctx, batch...); err != nil {
				return err
			}
			count += len(batch)
			batch = batch[:0]
			lastEntry = &entry
		}
		return nil
	}); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}
	if len(batch) > 0 {
		count += len(batch)
		if err := node.applyEntriesAndResolveCommands(ctx, batch...); err != nil {
			return err
		}
		lastEntry = pointer.For(batch[len(batch)-1])
	}
	if lastEntry != nil {
		logFields["last_entry_index"] = lastEntry.Index
	}
	logFields["count"] = count
	node.logger.
		WithFields(logFields).
		WithField("count", count).
		Infof("Replayed spool")

	return nil
}

func (node *Node) runMaintenanceTask(ctx context.Context, task func(ctx context.Context) (uint64, error)) {
	gatingTerminated := make(chan struct{})
	node.gatingTerminated = gatingTerminated

	node.taskExecutor.interrupt()
	node.taskExecutor.run(ctx, func(ctx context.Context) error {
		defer func() {
			close(gatingTerminated)
		}()

		frozenAtIndex, err := task(ctx)
		if err != nil {
			return err
		}

		return node.replaySpool(ctx, frozenAtIndex)
	})
}

// Apply proposes a command and waits for it to be applied, returning the applied index
// This is similar to hashicorp/raft's Apply() method
func (node *Node) Apply(ctx context.Context, cmd *raftcommand.Command) (any, error) {
	future := newFuture()
	start := time.Now()

	node.futures.Store(cmd.Id, future)
	defer func() {
		node.futures.Delete(cmd.Id)
		node.commandDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}()

	cmdData, err := proto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("marshaling command: %w", err)
	}

	proposal := newProposal(cmdData)

	select {
	case node.proposeCh <- proposal:
		node.proposeQueueLoadHistogram.Record(context.Background(), int64(node.proposeQueueInflight.Add(1)))
	default:
		node.logger.WithFields(map[string]any{
			"channel": "raft.node.propose",
		}).Errorf("Channel full")
		node.proposeQueueFullCounter.Add(context.Background(), 1)
		return nil, fmt.Errorf("propose channel full")
	}

	if _, err := proposal.wait(); err != nil {
		return nil, err
	}

	return future.wait()
}

func (node *Node) IsLeader() bool {
	lastSoftState := node.lastSoftState.Load()
	if lastSoftState == nil {
		return false
	}
	return lastSoftState.RaftState == raft.StateLeader
}

func (node *Node) GetLeader() uint64 {
	lastSoftState := node.lastSoftState.Load()
	if lastSoftState == nil {
		return 0
	}
	return lastSoftState.Lead
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

func (node *Node) Stop(ctx context.Context) error {
	node.logger.Infof("Stopping node")
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

type proposal struct {
	*future
	data []byte
}

func newProposal(data []byte) *proposal {
	return &proposal{
		data:   data,
		future: newFuture(),
	}
}
