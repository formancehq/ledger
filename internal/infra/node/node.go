package node

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"
)

const (
	statusNormal = iota
	statusSyncing
	statusSnapshotting
	statusOutOfSync
)

var (
	// ErrProposalQueueFull is returned when the proposal queue is full.
	ErrProposalQueueFull = fmt.Errorf("propose channel full")

	// ErrNotLeader is returned when a leadership transfer is attempted on a non-leader node.
	ErrNotLeader = fmt.Errorf("this node is not the leader")

	// ErrUnknownTransferee is returned when the transferee is not a known cluster member.
	ErrUnknownTransferee = fmt.Errorf("transferee is not a known cluster member")

	// ErrTransferLeaderTimeout is returned when leadership transfer does not complete in time.
	ErrTransferLeaderTimeout = fmt.Errorf("leadership transfer timed out")

	// ErrNodeAlreadyInCluster is returned when trying to add a node that already exists.
	ErrNodeAlreadyInCluster = fmt.Errorf("node already in cluster")

	// ErrLearnerNotEligible is returned when trying to transfer leadership to a learner.
	ErrLearnerNotEligible = fmt.Errorf("learner nodes are not eligible for leadership")

	// ErrCannotRemoveSelf is returned when trying to remove the leader node itself.
	ErrCannotRemoveSelf = fmt.Errorf("cannot remove the leader node; transfer leadership first")

	// ErrNodeNotInCluster is returned when trying to remove a node that is not a cluster member.
	ErrNodeNotInCluster = fmt.Errorf("node is not a member of the cluster")
)

// clusterCommand represents an operation that must execute in the orchestrate loop
// because rawNode is not thread-safe. Implementations return an error via errCh.
type clusterCommand struct {
	fn    func() error
	errCh chan error
}

// execClusterCommand dispatches a function to the orchestrate loop and waits for its result.
func (node *Node) execClusterCommand(ctx context.Context, fn func() error) error {
	cmd := &clusterCommand{
		fn:    fn,
		errCh: make(chan error, 1),
	}

	select {
	case node.clusterCommandCh <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-cmd.errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft
type Node struct {
	rawNode                 *raft.RawNode
	logger                  logging.Logger
	fsm                     *state.Machine
	wal                     wal.WAL
	transport               Transport
	config                  NodeConfig
	proposeCh               chan *Proposal
	clusterCommandCh        chan *clusterCommand
	confState               *raftpb.ConfState
	futures                 SyncMap[uint64, *futures.Future[state.ApplyResult]]
	lastSoftState           atomic.Pointer[raft.SoftState]
	snapshotFetcherProvider state.SnapshotFetcherProvider
	observer                *Observer

	store             *dal.Store
	spool             spool.Spool
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
	applyEntriesHistogram             metric.Int64Histogram
	applyEntriesBatchSizeCounter      metric.Int64Counter
	applyEntriesBatchSizeHistogram    metric.Int64Histogram
	processEntryHistogram             metric.Int64Histogram
	appendEntriesHistogram            metric.Int64Histogram
	leadMonitorHistogram              metric.Int64Gauge
	committedEntriesPerReadyHistogram metric.Int64Histogram
	createSnapshotHistogram           metric.Float64Histogram
	snapshotTriggeredCounter          metric.Int64Counter
	readyWaitDurationHistogram        metric.Int64Histogram
	readyTerminatedWaitHistogram      metric.Int64Histogram
	unspoolDurationHistogram          metric.Float64Histogram
	gatingWaitDurationHistogram       metric.Int64Histogram
	readiesDuringGatingHistogram      metric.Int64Histogram
	maintenanceSnapshotHistogram      metric.Float64Histogram
	maintenanceReplaySpoolHistogram   metric.Float64Histogram
}

// NewNode creates a new wrapper around a RawNode
func NewNode(
	cfg NodeConfig,
	transport Transport,
	store *dal.Store,
	logger logging.Logger,
	meter metric.Meter,
	spool spool.Spool,
	wal wal.WAL,
	snapshotFetcherProvider state.SnapshotFetcherProvider,
	fsm *state.Machine,
) (*Node, error) {

	cfg.SetDefaults()

	snapshot, err := wal.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	var initialConfState raftpb.ConfState
	if len(snapshot.Metadata.ConfState.Voters) == 0 {
		logger.Infof("Detected empty WAL, creating initial snapshot")

		// Check for RESTORED marker from a completed backup restore
		marker, err := ReadRestoredMarker(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("reading restored marker: %w", err)
		}

		if marker != nil {
			// Restore mode: bootstrap from restored data.
			// The backup was compacted: all attribute indices are 0 and lastAppliedIndex is 0.
			// We need to recover the FSM counters (nextLedgerID, nextSequenceID, etc.)
			// from the Pebble data before creating the WAL snapshot.
			logger.WithFields(map[string]any{
				"lastAppliedIndex":     marker.LastAppliedIndex,
				"lastAppliedTimestamp": marker.LastAppliedTimestamp,
			}).Infof("Detected RESTORED marker, bootstrapping from restored data")

			if err := fsm.RecoverState(); err != nil {
				return nil, fmt.Errorf("recovering FSM state from store: %w", err)
			}

			data, err := fsm.CreateSnapshot(context.Background())
			if err != nil {
				return nil, fmt.Errorf("creating restore snapshot data: %w", err)
			}

			initialConfState = raftpb.ConfState{
				Voters: []uint64{cfg.NodeID},
			}
			if err := wal.CreateSnapshot(marker.LastAppliedIndex, &initialConfState, data); err != nil {
				return nil, fmt.Errorf("creating restore snapshot: %w", err)
			}

			if err := RemoveRestoredMarker(cfg.DataDir); err != nil {
				return nil, fmt.Errorf("removing restored marker: %w", err)
			}

			logger.Infof("Restored bootstrap complete, marker removed")
		} else {
			var (
				voters   []uint64
				learners []uint64
			)

			if cfg.Bootstrap {
				// Bootstrap mode: this node + any known peers start as voters.
				voters = make([]uint64, 0, len(cfg.Peers)+1)
				voters = append(voters, cfg.NodeID)
				for _, peerEntry := range cfg.Peers {
					voters = append(voters, peerEntry.ID)
				}
			} else if len(cfg.Peers) > 0 {
				// Join mode: existing peers are voters, self joins as learner.
				// The leader will add us via ConfChange after we start.
				voters = make([]uint64, 0, len(cfg.Peers))
				for _, peerEntry := range cfg.Peers {
					voters = append(voters, peerEntry.ID)
				}
				learners = []uint64{cfg.NodeID}
			} else {
				return nil, fmt.Errorf("first start requires --bootstrap or --join")
			}

			data, err := fsm.CreateSnapshot(context.Background())
			if err != nil {
				return nil, fmt.Errorf("creating initial snapshot data: %w", err)
			}

			initialConfState = raftpb.ConfState{
				Voters:   voters,
				Learners: learners,
			}
			if err := wal.CreateSnapshot(0, &initialConfState, data); err != nil {
				return nil, fmt.Errorf("creating initial snapshot: %w", err)
			}
		}
	} else {
		if snapshot.Metadata.Index > 0 {
			logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring Machine from snapshot")
			if err := fsm.InstallSnapshot(context.Background(), snapshot); err != nil {
				panic(err)
			}

			logger.Infof("Snapshot restored successfully")
		} else {
			logger.Infof("Empty snapshot found, starting with empty Machine")
		}

		logger.Infof("Finished restoring Machine from storage")
		_, initialConfState, err = wal.InitialState()
		if err != nil {
			return nil, err
		}

		// Safety check: verify that cfg.NodeID is present in the ConfState.
		// If it's not, the node-id or wal-dir was likely changed between restarts,
		// which would make this node invisible to the cluster.
		if !confStateContainsNode(initialConfState, cfg.NodeID) {
			return nil, fmt.Errorf(
				"node-id %d not found in WAL ConfState (voters=%v, learners=%v); "+
					"this usually means --node-id or --wal-dir was changed between restarts",
				cfg.NodeID, initialConfState.Voters, initialConfState.Learners,
			)
		}
	}

	initialStatus := atomic.Int32{}
	initialStatus.Store(statusNormal)

	node := &Node{
		logger:                  logger,
		wal:                     wal,
		transport:               transport,
		config:                  cfg,
		proposeCh:               make(chan *Proposal, cfg.ProposeQueueCapacity),
		clusterCommandCh:        make(chan *clusterCommand, 1),
		store:                   store,
		fsm:                     fsm,
		spool:                   spool,
		snapshotThreshold:       cfg.SnapshotThreshold,
		compactionMargin:        cfg.CompactionMargin,
		status:                  &initialStatus,
		taskExecutor:            newSingleTaskExecutor(logger),
		confState:               &initialConfState,
		snapshotFetcherProvider: snapshotFetcherProvider,
		readies:                 make(chan raft.Ready, 1),
		readyTerminated:         make(chan raft.Ready, 1),
		tasks:                   newTaskSet(),
		stopChannel:             make(chan chan struct{}),
	}

	node.applyEntriesHistogram, err = meter.Int64Histogram("raft.apply_entries.duration",
		metric.WithDescription("Time spent applying entries to Machine"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 20000, 50000, 100000, 150000, 200000, 300000, 500000,
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
			0, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.snapshotTriggeredCounter, err = meter.Int64Counter("raft.snapshot.triggered",
		metric.WithDescription("Number of snapshots triggered"),
		metric.WithUnit("1"),
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

	node.readyTerminatedWaitHistogram, err = meter.Int64Histogram(
		"raft.node.ready_terminated.wait_duration",
		metric.WithDescription("Time spent waiting for orchestrate to consume readyTerminated"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.unspoolDurationHistogram, err = meter.Float64Histogram(
		"raft.node.unspool.duration",
		metric.WithDescription("Time spent in unspoolAndResume after a maintenance task (snapshot/checkpoint)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 20000, 50000, 100000, 250000, 500000, 1000000, 2000000, 5000000, 10000000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.gatingWaitDurationHistogram, err = meter.Int64Histogram(
		"raft.node.gating.wait_duration",
		metric.WithDescription("Time spent waiting for gatingTerminated (maintenance task completion)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.readiesDuringGatingHistogram, err = meter.Int64Histogram(
		"raft.node.gating.readies_processed",
		metric.WithDescription("Number of Readies processed during each gating period"),
		metric.WithUnit("1"),
		metric.WithExplicitBucketBoundaries(
			0, 1, 2, 3, 5, 10, 20, 50, 100, 200,
		),
	)
	if err != nil {
		panic(err)
	}

	node.maintenanceSnapshotHistogram, err = meter.Float64Histogram(
		"raft.node.maintenance.snapshot_creation.duration",
		metric.WithDescription("Time spent creating the snapshot during a maintenance task (excluding replay spool)"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
		),
	)
	if err != nil {
		panic(err)
	}

	node.maintenanceReplaySpoolHistogram, err = meter.Float64Histogram(
		"raft.node.maintenance.replay_spool.duration",
		metric.WithDescription("Time spent replaying spooled entries after snapshot creation in a maintenance task"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000, 5000000,
		),
	)
	if err != nil {
		panic(err)
	}

	// Check if store is up to date and replay spool if needed
	isStoreUpToDate, err := fsm.IsStoreUpToDate(context.Background())
	if err != nil {
		return nil, fmt.Errorf("checking if store is up to date: %w", err)
	}

	if !isStoreUpToDate {
		logger.Infof("Store is not up to date, resuming from snapshot and tagging node as out of sync")
		node.status.Store(statusOutOfSync)
	} else {
		// Recovery: if a period is in CLOSING state but no seal checkpoint exists,
		// the node crashed after ClosePeriod batch.Commit() but before checkpoint creation.
		// Pebble state is exactly at the ClosePeriod boundary right now (spool replay hasn't run).
		if period := fsm.ClosingPeriod(); period != nil {
			if _, exists := store.TemporaryCheckpointPath("seal"); !exists {
				logger.Infof("Recovering: creating seal checkpoint for closing period %d", period.Id)
				checkpointPath, err := store.CreateTemporaryCheckpoint("seal")
				if err != nil {
					return nil, fmt.Errorf("creating recovery seal checkpoint: %w", err)
				}
				req := state.SealRequestFromPeriod(period)
				req.CheckpointPath = checkpointPath
				select {
				case fsm.SealRequestCh() <- *req:
				default:
				}
			}
		}

		storeLastAppliedIndex, err := query.ReadLastAppliedIndex(store)
		if err != nil {
			return nil, fmt.Errorf("getting store last applied index: %w", err)
		}

		if err := node.replaySpool(context.Background(), storeLastAppliedIndex); err != nil {
			return nil, fmt.Errorf("replaying spool: %w", err)
		}
	}

	return node, nil
}

func (node *Node) Run(ctx context.Context, ready chan struct{}) error {

	raftConfig := &raft.Config{
		ID:                        node.config.NodeID,
		ElectionTick:              node.config.ElectionTick,
		HeartbeatTick:             node.config.HeartbeatTick,
		Storage:                   node.wal,
		MaxSizePerMsg:             node.config.MaxSizePerMsg,
		MaxInflightMsgs:           node.config.MaxInflightMsgs,
		Logger:                    NewLoggerAdapter(node.logger),
		DisableProposalForwarding: true,
		PreVote:                   true,
	}

	node.logger.WithFields(map[string]any{
		"config": raftConfig,
	}).Infof("Starting raft node")

	var err error
	node.rawNode, err = raft.NewRawNode(raftConfig)
	if err != nil {
		return fmt.Errorf("creating raw rawNode: %w", err)
	}

	node.tasks.add(newTask(node.orchestrate))
	node.tasks.add(newTask(node.processReadies))
	node.tasks.run(ctx)

	close(ready)

	select {
	case ch := <-node.stopChannel:
		if err := node.tasks.stop(); err != nil {
			node.logger.Errorf("Error stopping task pool: %v", err)
		}
		close(ch)
		return nil
	case err := <-node.tasks.err():
		if stopErr := node.tasks.stop(); stopErr != nil {
			node.logger.Errorf("Error stopping remaining tasks after failure: %v", stopErr)
		}
		return fmt.Errorf("task pool error: %w", err)
	}
}

func (node *Node) processReadies(ctx context.Context, stop chan struct{}) error {
	var (
		readiesDuringGating int64
		gatingStart         time.Time
	)

	for {
		waitStart := time.Now()
		select {
		case rd := <-node.readies:
			node.readyWaitDurationHistogram.Record(context.Background(), time.Since(waitStart).Microseconds())
			if !gatingStart.IsZero() {
				readiesDuringGating++
			}
			now := time.Now()
			err := node.processReady(ctx, rd)
			node.processEntryHistogram.Record(context.Background(), time.Since(now).Microseconds())
			if err != nil {
				return err
			}
			// Detect if gating just started during processReady
			if node.gatingTerminated != nil && gatingStart.IsZero() {
				gatingStart = time.Now()
			}
			terminatedStart := time.Now()
			select {
			case node.readyTerminated <- rd:
				node.readyTerminatedWaitHistogram.Record(context.Background(), time.Since(terminatedStart).Microseconds())
			case <-stop:
				return nil
			}
		case <-node.gatingTerminated:
			node.gatingWaitDurationHistogram.Record(context.Background(), time.Since(gatingStart).Microseconds())
			node.readiesDuringGatingHistogram.Record(context.Background(), readiesDuringGating)
			readiesDuringGating = 0
			gatingStart = time.Time{}
			// Drain the spool in this goroutine - no race condition possible
			// since this is the same goroutine that does spooling
			unspoolStart := time.Now()
			if err := node.unspoolAndResume(ctx); err != nil {
				return err
			}
			node.unspoolDurationHistogram.Record(context.Background(), float64(time.Since(unspoolStart).Microseconds()))
			node.gatingTerminated = nil
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
		// Only trigger sync from SoftState if this Ready does NOT also contain
		// a snapshot. When both are present, the snapshot processing below will
		// trigger its own syncSnapshot. Doing it here too would start a background
		// task that is immediately interrupted by the second syncSnapshot call,
		// which corrupts the spool read cache (entries read but never applied).
		if ss.Lead != 0 && node.status.Load() == statusOutOfSync && raft.IsEmptySnap(rd.Snapshot) {
			node.syncSnapshot(ctx, ss.Lead)
		}

		actualNodeLastSoftState := node.lastSoftState.Load()
		wasLeader := actualNodeLastSoftState != nil && actualNodeLastSoftState.RaftState == raft.StateLeader
		isLeader := ss.RaftState == raft.StateLeader

		if wasLeader != isLeader {
			status := node.rawNode.Status()
			logger := node.logger.WithFields(map[string]any{
				"lead": ss.Lead,
				"term": status.Term,
			})

			// leadership loss
			if wasLeader && !isLeader {
				logger.Infof("Leadership lost")
				if node.observer != nil {
					node.observer.Emit(LeadershipChangeEvent{IsLeader: false})
				}
			}
			// acquire leadership
			if !wasLeader && isLeader {
				logger.Infof("Leadership gained")
				if node.observer != nil {
					node.observer.Emit(LeadershipChangeEvent{IsLeader: true})
				}
				node.fsm.OnLeadershipAcquired()
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

		// The snapshot is already persisted in WAL at this point. If syncSnapshot
		// fails (network issue, leader unavailable, etc.), the node transitions to
		// statusOutOfSync and will retry automatically when a leader is detected
		// via SoftState or on restart (isStoreUpToDate check).
		node.syncSnapshot(ctx, node.lastSoftState.Load().Lead)
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
			// V1→V2 conversion does not copy Context; propagate it manually.
			cc.Context = ccV1.Context
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

		// Notify observers about configuration changes
		if node.observer != nil {
			for _, change := range cc.Changes {
				node.observer.Emit(ConfChangeEvent{
					NodeID:     change.NodeID,
					ChangeType: change.Type,
					Context:    cc.Context,
				})
			}
		}
	}

	if len(rd.CommittedEntries) > 0 {
		switch node.status.Load() {
		case statusNormal:
			err := node.applyEntriesToFSM(ctx, node.confState, rd.CommittedEntries...)
			if err != nil {
				return fmt.Errorf("applying entries to Machine: %w", err)
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

func (node *Node) applyEntriesAndResolveCommands(ctx context.Context, entries ...raftpb.Entry) (*state.ApplyEntriesResult, error) {
	start := time.Now()
	result, err := node.fsm.ApplyEntries(ctx, entries...)
	if err != nil {
		return nil, fmt.Errorf("applying entries to Machine: %w", err)
	}
	node.applyEntriesHistogram.Record(ctx, time.Since(start).Microseconds())
	node.applyEntriesBatchSizeCounter.Add(ctx, int64(len(result.Results)))
	node.applyEntriesBatchSizeHistogram.Record(ctx, int64(len(result.Results)))

	// Resolve all proposal futures. When CheckpointRequired, the last result
	// is the checkpoint-triggering entry — its future is resolved later by
	// handleCheckpointRequired once the checkpoint path is known.
	resolveCount := len(result.Results)
	if result.CheckpointRequired && resolveCount > 0 {
		resolveCount--
	}
	for _, r := range result.Results[:resolveCount] {
		future, exists := node.futures.Load(r.ProposalID)
		if !exists {
			continue
		}
		future.Resolve(r, r.Error)
		node.futures.Delete(r.ProposalID)
	}

	return result, nil
}

// applyEntriesToFSM applies entries directly to the Machine
func (node *Node) applyEntriesToFSM(ctx context.Context, confState *raftpb.ConfState, entries ...raftpb.Entry) error {
	result, err := node.applyEntriesAndResolveCommands(ctx, entries...)
	if err != nil {
		return err
	}

	// If Machine stopped at a checkpoint boundary (ClosePeriod or CreateCheckpoint),
	// enter maintenance mode and create the checkpoint off the Raft hot path.
	if result.CheckpointRequired {
		return node.handleCheckpointRequired(ctx, entries, result)
	}

	lastSnapshot, err := node.wal.Snapshot()
	if err != nil {
		panic(fmt.Errorf("getting last snapshot: %w", err))
	}

	if entries[len(entries)-1].Index-lastSnapshot.Metadata.Index >= node.snapshotThreshold {
		node.triggerSnapshot(ctx, confState, entries[len(entries)-1].Index, lastSnapshot.Metadata.Index)
	}

	return nil
}

// handleCheckpointRequired enters maintenance mode to create a checkpoint off
// the Raft hot path. Handles both ClosePeriod (seal checkpoint) and
// CreateCheckpoint (backup checkpoint). While the checkpoint is being created,
// new committed entries are spooled and replayed afterward.
func (node *Node) handleCheckpointRequired(
	ctx context.Context,
	entries []raftpb.Entry,
	applyResult *state.ApplyEntriesResult,
) error {
	// Spool remaining entries — they'll be replayed after the maintenance task
	if len(applyResult.RemainingEntries) > 0 {
		if err := node.spool.AppendCommittedEntries(ctx, applyResult.RemainingEntries...); err != nil {
			return fmt.Errorf("spooling remaining entries: %w", err)
		}
	}

	// Last applied index: the boundary entry (before any remaining entries)
	var frozenAtIndex uint64
	if len(applyResult.RemainingEntries) > 0 {
		frozenAtIndex = applyResult.RemainingEntries[0].Index - 1
	} else {
		frozenAtIndex = entries[len(entries)-1].Index
	}

	node.status.Store(statusSnapshotting)

	// Resolve the deferred future for the checkpoint-triggering proposal.
	// The last result in applyResult.Results is the entry that set CheckpointRequired.
	var deferredResult *state.ApplyResult
	var deferredFuture *futures.Future[state.ApplyResult]
	if len(applyResult.Results) > 0 {
		deferredResult = &applyResult.Results[len(applyResult.Results)-1]
		if f, ok := node.futures.Load(deferredResult.ProposalID); ok {
			deferredFuture = f
			node.futures.Delete(deferredResult.ProposalID)
		}
	}

	node.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		path, err := node.store.CreateTemporaryCheckpoint("checkpoint")
		if err != nil {
			if deferredFuture != nil {
				deferredFuture.Resolve(state.ApplyResult{}, err)
			}
			return 0, fmt.Errorf("creating checkpoint: %w", err)
		}

		if applyResult.OnCheckpointDone != nil {
			applyResult.OnCheckpointDone(path)
		}

		if deferredFuture != nil {
			deferredResult.CheckpointPath = path
			deferredFuture.Resolve(*deferredResult, nil)
		}

		return frozenAtIndex, nil
	}, nil)

	return nil
}

// triggerSnapshot creates a Raft snapshot when the threshold is reached.
func (node *Node) triggerSnapshot(ctx context.Context, confState *raftpb.ConfState, lastEntryIndex, lastSnapshotIndex uint64) {
	node.snapshotTriggeredCounter.Add(ctx, 1)
	node.status.Store(statusSnapshotting)

	node.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		node.logger.WithFields(map[string]any{
			"applied":           lastEntryIndex,
			"lastSnapshotIndex": lastSnapshotIndex,
			"snapshotThreshold": node.snapshotThreshold,
			"compactionMargin":  node.compactionMargin,
		}).Infof("Creating new snapshot")

		startTime := time.Now()
		snapshotData, err := node.fsm.CreateSnapshot(ctx)
		if err != nil {
			return 0, err
		}

		if err := node.wal.CreateSnapshot(lastEntryIndex, confState, snapshotData); err != nil {
			return 0, err
		}
		node.createSnapshotHistogram.Record(ctx, float64(time.Since(startTime).Milliseconds()))

		return lastEntryIndex, nil
	}, func(ctx context.Context) {
		// WAL compaction runs after gating ends to avoid holding the WAL
		// mutex during the spooling window, which would block wal.Append
		// and stall the Ready pipeline.
		if lastEntryIndex > node.compactionMargin {
			if err := node.wal.Compact(lastEntryIndex - node.compactionMargin); err != nil && !errors.Is(err, raft.ErrCompacted) {
				node.logger.WithFields(map[string]any{
					"error": err,
				}).Errorf("Failed to compact WAL")
			}
		}
	})
}

func (node *Node) orchestrate(ctx context.Context, stop chan struct{}) error {

	tickInterval := node.config.TickInterval
	if tickInterval == 0 {
		tickInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	processingTickInterval := node.config.ProcessingTickInterval
	if processingTickInterval == 0 {
		processingTickInterval = tickInterval / 10
	}
	processingTick := time.NewTicker(processingTickInterval)
	defer processingTick.Stop()

	// Helper to process a batch of messages.
	// Filters out MsgTimeoutNow while the node is syncing to prevent a
	// not-yet-caught-up node from being forced into leadership.
	stepMessages := func(msgs []raftpb.Message) error {
		for _, msg := range msgs {
			if msg.Type == raftpb.MsgTimeoutNow && node.isSyncing() {
				node.logger.Infof("Rejecting MsgTimeoutNow while syncing")
				continue
			}
			if err := node.rawNode.Step(msg); err != nil {
				if errors.Is(err, raft.ErrStepPeerNotFound) {
					node.logger.Debugf("Ignoring message from unknown peer %d (type=%s)", msg.From, msg.Type)
					continue
				}
				return err
			}
		}
		return nil
	}

	// Check HasReady immediately after stepping messages to avoid the
	// processingTick delay (~10ms) in the commit pipeline. Without this,
	// follower responses that make entries committable aren't detected
	// until the next processingTick, adding up to 10ms to every commit cycle.
	maybeCreateReady := func() {
		if node.readyTerminated == nil && node.rawNode.HasReady() {
			node.readyTerminated = make(chan raft.Ready, 1)
			processingTick.Stop()
			node.readies <- node.rawNode.Ready()
		}
	}

	for {
		select {
		case <-ticker.C:
			// Prevent election timeouts from happening while syncing the Machine
			status := node.status.Load()
			if status != statusSyncing && status != statusOutOfSync {
				node.rawNode.Tick()
				if node.config.AutoPromoteThreshold > 0 {
					node.checkAndPromoteLearners()
				}
			}
		case msgs := <-node.transport.RecvHighPriority():
			if err := stepMessages(msgs); err != nil {
				return err
			}
			maybeCreateReady()
		default:
			select {
			case msgs := <-node.transport.RecvHighPriority():
				if err := stepMessages(msgs); err != nil {
					return err
				}
				maybeCreateReady()
			case msgs := <-node.transport.RecvMediumPriority():
				if err := stepMessages(msgs); err != nil {
					return err
				}
				maybeCreateReady()
			case p := <-node.proposeCh:
				p.Resolve(nil, node.rawNode.Propose(p.data))
			case cmd := <-node.clusterCommandCh:
				cmd.errCh <- cmd.fn()
			default:
				select {
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
					maybeCreateReady()
				case msgs := <-node.transport.RecvMediumPriority():
					if err := stepMessages(msgs); err != nil {
						return err
					}
					maybeCreateReady()
				case msgs := <-node.transport.RecvLowPriority():
					if err := stepMessages(msgs); err != nil {
						return err
					}
					maybeCreateReady()
				case p := <-node.proposeCh:
					p.Resolve(nil, node.rawNode.Propose(p.data))
				case cmd := <-node.clusterCommandCh:
					cmd.errCh <- cmd.fn()
				case err := <-node.taskExecutor.error():
					return fmt.Errorf("task executor error: %w", err)
				}
			}
		}
	}
}

func (node *Node) unspoolAndResume(ctx context.Context) error {
	node.logger.Infof("Background operation terminated, applying spooled entries before resuming...")

	lastAppliedIndex, err := query.ReadLastAppliedIndex(node.store)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}

	if err := node.replaySpool(ctx, lastAppliedIndex); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}

	node.status.Store(statusNormal)

	lastAppliedIndex, err = query.ReadLastAppliedIndex(node.store)
	if err != nil {
		return fmt.Errorf("getting last applied index: %w", err)
	}
	if err := node.spool.Prune(lastAppliedIndex); err != nil {
		return fmt.Errorf("pruning spool: %w", err)
	}

	node.logger.Infof("Unspooling operation terminated, resuming...")

	return nil
}

// syncSnapshot starts a background synchronization with the leader.
// On failure the node transitions to statusOutOfSync so that new entries
// are spooled and a retry is triggered when a leader reappears in SoftState.
func (node *Node) syncSnapshot(ctx context.Context, leader uint64) {
	node.logger.
		WithFields(map[string]any{
			"leader": leader,
		}).
		Infof("Syncing snapshot from leader")

	node.status.Store(statusSyncing)

	node.runMaintenanceTask(ctx, func(ctx context.Context) (uint64, error) {
		snapshotFetcher, err := node.snapshotFetcherProvider.GetForPeer(leader)
		if err != nil {
			node.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to get snapshot fetcher, marking node as out of sync")
			node.status.Store(statusOutOfSync)
			return 0, nil
		}
		if _, err := node.fsm.SynchronizeWithLeader(ctx, snapshotFetcher); err != nil {
			node.logger.WithFields(map[string]any{
				"leader": leader,
				"error":  err,
			}).Errorf("Failed to synchronize with leader, marking node as out of sync")
			node.status.Store(statusOutOfSync)
			return 0, nil
		}
		return 0, nil
	}, nil)
}

func (node *Node) replaySpool(ctx context.Context, fromIndex uint64) error {

	node.logger.WithFields(map[string]any{
		"fromIndex": fromIndex,
	}).Infof("Replaying spool")

	until, err := node.spool.End()
	if err != nil {
		return fmt.Errorf("getting spool end position: %w", err)
	}

	count := 0
	batchSize := node.config.ReplayBatchSize
	batch := make([]raftpb.Entry, 0, batchSize)
	logFields := map[string]any{}
	var lastEntry *raftpb.Entry
	if err := node.spool.ReplayUntil(ctx, *until, fromIndex, func(entry raftpb.Entry) error {
		batch = append(batch, entry)
		if len(batch) >= batchSize {
			result, err := node.applyEntriesAndResolveCommands(ctx, batch...)
			if err != nil {
				return err
			}
			count += len(batch)
			batch = batch[:0]
			lastEntry = &entry

			// Handle checkpoint during replay (ClosePeriod or CreateCheckpoint)
			if result.CheckpointRequired {
				if err := node.handleCheckpointDuringReplay(ctx, result); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("replaying spool: %w", err)
	}
	if len(batch) > 0 {
		count += len(batch)
		result, err := node.applyEntriesAndResolveCommands(ctx, batch...)
		if err != nil {
			return err
		}
		lastEntry = pointer.For(batch[len(batch)-1])

		// Handle checkpoint during replay (ClosePeriod or CreateCheckpoint)
		if result.CheckpointRequired {
			if err := node.handleCheckpointDuringReplay(ctx, result); err != nil {
				return err
			}
		}
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

// handleCheckpointDuringReplay creates a temporary checkpoint and calls the
// FSM-provided callback when a checkpoint-requiring entry (ClosePeriod or
// CreateCheckpoint) is encountered during spool replay.
// Unlike handleCheckpointRequired, this does not enter maintenance mode — the
// checkpoint is created synchronously (acceptable since we're already off
// the hot path) and remaining entries are applied directly.
func (node *Node) handleCheckpointDuringReplay(ctx context.Context, applyResult *state.ApplyEntriesResult) error {
	checkpointPath, err := node.store.CreateTemporaryCheckpoint("replay")
	if err != nil {
		return fmt.Errorf("creating checkpoint during replay: %w", err)
	}

	if applyResult.OnCheckpointDone != nil {
		applyResult.OnCheckpointDone(checkpointPath)
	}

	// Apply remaining entries directly (no re-spool needed since we're replaying)
	if len(applyResult.RemainingEntries) > 0 {
		_, err := node.applyEntriesAndResolveCommands(ctx, applyResult.RemainingEntries...)
		if err != nil {
			return fmt.Errorf("applying remaining entries after checkpoint during replay: %w", err)
		}
	}

	return nil
}

func (node *Node) runMaintenanceTask(
	ctx context.Context,
	task func(ctx context.Context) (uint64, error),
	postGating func(ctx context.Context),
) {
	gatingTerminated := make(chan struct{})
	node.gatingTerminated = gatingTerminated

	node.taskExecutor.interrupt()
	node.taskExecutor.run(ctx, func(ctx context.Context) error {
		snapshotStart := time.Now()
		frozenAtIndex, err := task(ctx)
		if err != nil {
			close(gatingTerminated)
			return err
		}
		node.maintenanceSnapshotHistogram.Record(context.Background(), float64(time.Since(snapshotStart).Microseconds()))

		replayStart := time.Now()
		if err := node.replaySpool(ctx, frozenAtIndex); err != nil {
			close(gatingTerminated)
			return err
		}
		node.maintenanceReplaySpoolHistogram.Record(context.Background(), float64(time.Since(replayStart).Microseconds()))

		// End gating before post-gating work (e.g. WAL compaction).
		// Post-gating work doesn't need the FSM to be frozen and would
		// unnecessarily extend the spooling window, increasing latency.
		close(gatingTerminated)

		if postGating != nil {
			postGating(ctx)
		}

		return nil
	})
}

// handleTransferLeader validates preconditions and calls rawNode.TransferLeader.
// Must be called from the orchestrate loop (rawNode is not thread-safe).
func (node *Node) handleTransferLeader(transferee uint64) error {
	status := node.rawNode.Status()

	if status.RaftState != raft.StateLeader {
		return ErrNotLeader
	}

	prog, ok := status.Progress[transferee]
	if !ok {
		return ErrUnknownTransferee
	}

	if prog.IsLearner {
		return ErrLearnerNotEligible
	}

	node.rawNode.TransferLeader(transferee)
	return nil
}

// ProposeBackupCheckpoint proposes a CreateCheckpoint command through Raft consensus.
// The checkpoint is created during maintenance mode off the Raft hot path while no
// new proposals are being applied, guaranteeing a consistent snapshot.
// Dirty boundaries are written into the checkpoint copy (not the live DB).
// Returns the filesystem path to the created checkpoint.
func (node *Node) ProposeBackupCheckpoint(ctx context.Context) (string, error) {
	cmd := commands.NewCommand()
	cmd.CreateCheckpoint = true

	cmdData, err := cmd.MarshalVT()
	if err != nil {
		return "", fmt.Errorf("marshaling checkpoint proposal: %w", err)
	}

	proposal := NewProposal(cmd.Id, cmdData)
	fsmFuture, err := node.Propose(proposal)
	if err != nil {
		return "", fmt.Errorf("proposing checkpoint: %w", err)
	}

	// Wait for Raft consensus
	if _, err := proposal.Wait(); err != nil {
		return "", fmt.Errorf("waiting for checkpoint raft consensus: %w", err)
	}

	// Wait for checkpoint creation (resolved by handleCheckpointRequired)
	result, err := fsmFuture.Wait()
	if err != nil {
		return "", fmt.Errorf("waiting for checkpoint creation: %w", err)
	}

	return result.CheckpointPath, nil
}

// TransferLeader initiates a leadership transfer to the given node.
// It dispatches the request to the orchestrate loop (since rawNode is not thread-safe)
// and then polls lastSoftState to confirm the leader has changed.
func (node *Node) TransferLeader(ctx context.Context, transferee uint64) error {
	// No-op if transferee is this node and we're already leader
	if transferee == node.config.NodeID && node.IsLeader() {
		return nil
	}

	if err := node.execClusterCommand(ctx, func() error {
		return node.handleTransferLeader(transferee)
	}); err != nil {
		return err
	}

	// Poll lastSoftState to confirm the leader has changed
	timeout := time.Duration(2*node.config.ElectionTick) * node.config.TickInterval
	if timeout < 2*time.Second {
		timeout = 2 * time.Second
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()

	for {
		select {
		case <-deadline.C:
			return ErrTransferLeaderTimeout
		case <-poll.C:
			ss := node.lastSoftState.Load()
			if ss != nil && ss.Lead == transferee {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Propose implements the Proposer interface.
func (node *Node) Propose(proposal *Proposal) (*futures.Future[state.ApplyResult], error) {
	// Create a separate future for Machine results.
	// The proposal's embedded Future is for Raft consensus (resolved by rawNode.Propose).
	// The fsmFuture is for Machine processing (resolved when entry is applied).
	fsmFuture := futures.New[state.ApplyResult]()
	node.futures.Store(proposal.commandID, fsmFuture)

	select {
	case node.proposeCh <- proposal:
		return fsmFuture, nil
	default:
		node.futures.Delete(proposal.commandID)
		return nil, ErrProposalQueueFull
	}
}

func (node *Node) InitialIndex() uint64 {
	ret, err := node.wal.LastIndex()
	if err != nil {
		panic(err)
	}
	return ret + 1
}

func (node *Node) IsLeader() bool {
	lastSoftState := node.lastSoftState.Load()
	if lastSoftState == nil {
		return false
	}
	return lastSoftState.RaftState == raft.StateLeader
}

// isSyncing returns true when the node is restoring a snapshot or checkpoint
// and is not yet ready to serve as leader.
func (node *Node) isSyncing() bool {
	s := node.status.Load()
	return s == statusSyncing || s == statusSnapshotting || s == statusOutOfSync
}

func (node *Node) GetLeader() uint64 {
	lastSoftState := node.lastSoftState.Load()
	if lastSoftState == nil {
		return 0
	}
	return lastSoftState.Lead
}

// GetNodeID returns the ID of this node
func (node *Node) GetNodeID() uint64 {
	return node.config.NodeID
}

// GetClusterState returns the current state of the Raft cluster
func (node *Node) GetClusterState(ctx context.Context) (*clusterpb.ClusterState, error) {
	status := node.rawNode.Status()

	// Get leader
	leaderID := status.Lead

	stateStr := status.RaftState.String()

	// Build progress information map and nodes list only if this node is the leader
	var nodes []*clusterpb.NodeInfo
	progress := make(map[uint64]*clusterpb.ProgressInfo)

	if status.RaftState == raft.StateLeader {
		// Build progress information map first
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

			progress[id] = &clusterpb.ProgressInfo{
				Match:           prog.Match,
				Next:            prog.Next,
				State:           stateStr,
				PendingSnapshot: prog.PendingSnapshot,
				RecentActive:    prog.RecentActive,
				ProbeSent:       prog.ProbeSent,
				IsPaused:        prog.IsPaused(),
				IsLearner:       prog.IsLearner,
			}
		}

		// Build nodes list with progress information
		nodes = make([]*clusterpb.NodeInfo, 0, len(status.Progress))
		for id, prog := range status.Progress {
			suffrage := "Voter"
			if prog.IsLearner {
				suffrage = "Learner"
			}

			nodeInfo := &clusterpb.NodeInfo{
				Id:       uint32(id),
				Suffrage: suffrage,
			}

			if progressInfo, ok := progress[id]; ok {
				nodeInfo.Progress = progressInfo
			}

			nodes = append(nodes, nodeInfo)
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
	raftStatus := &clusterpb.RaftStatus{
		State:     stateStr,
		Term:      hardState.Term,
		Leader:    leaderID,
		Applied:   status.Applied,
		Commit:    hardState.Commit,
		LastIndex: lastIndex,
		Vote:      hardState.Vote,
		Progress:  progress,
	}

	return &clusterpb.ClusterState{
		State:      stateStr,
		Leader:     uint32(leaderID),
		Nodes:      nodes,
		LocalNode:  uint32(node.config.NodeID),
		RaftStatus: raftStatus,
	}, nil
}

// IsHealthy returns true if the rawNode is connected to the cluster (leader or follower)
func (node *Node) IsHealthy() bool {
	status := node.rawNode.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

// pickBestTransferee selects the follower with the highest Match index (most synchronized).
// Must be dispatched via execClusterCommand because rawNode is not thread-safe.
func (node *Node) pickBestTransferee(ctx context.Context) (uint64, error) {
	var best uint64
	err := node.execClusterCommand(ctx, func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}
		var bestMatch uint64
		for id, prog := range status.Progress {
			if id == node.config.NodeID {
				continue
			}
			// Skip learner nodes – they cannot become leader
			if prog.IsLearner {
				continue
			}
			if prog.Match > bestMatch {
				bestMatch = prog.Match
				best = id
			}
		}
		return nil
	})
	return best, err
}

// tryTransferLeadershipBeforeShutdown attempts a best-effort leadership transfer
// before shutting down. If the transfer fails for any reason, the shutdown continues
// normally and the cluster will elect a new leader via the standard election mechanism.
func (node *Node) tryTransferLeadershipBeforeShutdown(ctx context.Context) {
	transferee, err := node.pickBestTransferee(ctx)
	if err != nil || transferee == 0 {
		node.logger.Infof("No eligible transferee, skipping pre-shutdown leadership transfer")
		return
	}

	node.logger.WithFields(map[string]any{"transferee": transferee}).
		Infof("Attempting leadership transfer before shutdown")

	if err := node.TransferLeader(ctx, transferee); err != nil {
		node.logger.WithFields(map[string]any{"error": err}).
			Errorf("Pre-shutdown leadership transfer failed, proceeding with normal shutdown")
		return
	}

	node.logger.Infof("Leadership transferred successfully before shutdown")
}

func (node *Node) Stop(ctx context.Context) error {
	node.logger.Infof("Stopping node")

	if node.IsLeader() {
		node.tryTransferLeadershipBeforeShutdown(ctx)
	}

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

// SetObserver sets the observer that receives events emitted by the node.
func (node *Node) SetObserver(obs *Observer) {
	node.observer = obs
}

// AddLearner proposes adding a non-voting learner node to the Raft cluster.
// Must be called on the leader.
func (node *Node) AddLearner(ctx context.Context, nodeID uint64, raftAddr, serviceAddr string) error {
	ccCtx, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	})
	if err != nil {
		return fmt.Errorf("marshaling conf change context: %w", err)
	}

	return node.execClusterCommand(ctx, func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}

		// Check if node already exists in the cluster
		if _, ok := status.Progress[nodeID]; ok {
			return ErrNodeAlreadyInCluster
		}

		cc := raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeAddLearnerNode,
					NodeID: nodeID,
				},
			},
			Context: ccCtx,
		}

		return node.rawNode.ProposeConfChange(cc)
	})
}

// PromoteLearner proposes promoting a learner node to a full voter.
// Must be called on the leader.
func (node *Node) PromoteLearner(ctx context.Context, nodeID uint64) error {
	return node.execClusterCommand(ctx, func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}

		prog, ok := status.Progress[nodeID]
		if !ok {
			return fmt.Errorf("node %d is not a known cluster member", nodeID)
		}
		if !prog.IsLearner {
			return fmt.Errorf("node %d is already a voter", nodeID)
		}

		cc := raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{
				{
					Type:   raftpb.ConfChangeAddNode,
					NodeID: nodeID,
				},
			},
		}

		return node.rawNode.ProposeConfChange(cc)
	})
}

// RemoveNode proposes removing a node (voter or learner) from the Raft cluster.
// Must be called on the leader. Cannot remove the leader itself.
func (node *Node) RemoveNode(ctx context.Context, nodeID uint64) error {
	return node.execClusterCommand(ctx, func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}

		if nodeID == node.config.NodeID {
			return ErrCannotRemoveSelf
		}

		if _, ok := status.Progress[nodeID]; !ok {
			return ErrNodeNotInCluster
		}

		cc := raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: nodeID,
			}},
		}

		return node.rawNode.ProposeConfChange(cc)
	})
}

// checkAndPromoteLearners checks all learner nodes and promotes those that are
// caught up (within AutoPromoteThreshold of the commit index).
// Must be called from the orchestrate loop (rawNode is not thread-safe).
func (node *Node) checkAndPromoteLearners() {
	status := node.rawNode.Status()
	if status.RaftState != raft.StateLeader {
		return
	}

	for id, prog := range status.Progress {
		if !prog.IsLearner || !prog.RecentActive || prog.Match == 0 {
			continue
		}
		if prog.Match+node.config.AutoPromoteThreshold >= status.Commit {
			node.logger.WithFields(map[string]any{
				"node_id":   id,
				"match":     prog.Match,
				"commit":    status.Commit,
				"threshold": node.config.AutoPromoteThreshold,
			}).Infof("Auto-promoting learner to voter")

			cc := raftpb.ConfChangeV2{
				Changes: []raftpb.ConfChangeSingle{
					{
						Type:   raftpb.ConfChangeAddNode,
						NodeID: id,
					},
				},
			}
			if err := node.rawNode.ProposeConfChange(cc); err != nil {
				node.logger.WithFields(map[string]any{
					"node_id": id,
					"error":   err,
				}).Errorf("Failed to propose learner promotion")
			}
		}
	}
}

// confStateContainsNode returns true if nodeID appears in the ConfState's
// Voters or Learners list.
func confStateContainsNode(cs raftpb.ConfState, nodeID uint64) bool {
	for _, id := range cs.Voters {
		if id == nodeID {
			return true
		}
	}
	for _, id := range cs.Learners {
		if id == nodeID {
			return true
		}
	}
	return false
}

type Proposal struct {
	*futures.Future[any]
	commandID uint64
	data      []byte
}

func NewProposal(commandID uint64, data []byte) *Proposal {
	return &Proposal{
		commandID: commandID,
		data:      data,
		Future:    futures.New[any](),
	}
}

// Data returns the serialized proposal data.
func (p *Proposal) Data() []byte {
	return p.data
}
