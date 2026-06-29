package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

const (
	statusNormal             = iota
	statusGated              // maintenance task in progress, entries are spooled
	statusOutOfSync          // node needs leader sync
	statusInstallingSnapshot // processReadies is installing a snapshot, Run must not touch FSM
)

// gatingReason constants — stored in an atomic.Int32 for race-free observability.
const (
	gatingReasonNone            int32 = iota
	gatingReasonSyncing               // leader sync in progress
	gatingReasonSnapshotting          // checkpoint creation in progress
	gatingReasonQueryCheckpoint       // query-checkpoint in progress
)

var (
	// ErrNotLeader is returned when a leadership transfer is attempted on a non-leader node.
	ErrNotLeader = errors.New("this node is not the leader")

	// ErrLeadershipLost is returned to pending FSM futures whose proposals were
	// truncated by a later Raft term (issue #172). Distinct from ErrNotLeader
	// (returned synchronously when admission refuses a proposal on a
	// non-leader) and from raft.ErrProposalDropped (the proposal never reached
	// rawNode's log).
	ErrLeadershipLost = errors.New("leadership lost: proposal was truncated by a later term")

	// ErrUnknownTransferee is returned when the transferee is not a known cluster member.
	ErrUnknownTransferee = errors.New("transferee is not a known cluster member")

	// ErrTransferLeaderTimeout is returned when leadership transfer does not complete in time.
	ErrTransferLeaderTimeout = errors.New("leadership transfer timed out")

	// ErrNodeAlreadyInCluster is returned when trying to add a node that already exists.
	ErrNodeAlreadyInCluster = errors.New("node already in cluster")

	// ErrLearnerNotEligible is returned when trying to transfer leadership to a learner.
	ErrLearnerNotEligible = errors.New("learner nodes are not eligible for leadership")

	// ErrCannotRemoveSelf is returned when trying to remove the leader node itself.
	ErrCannotRemoveSelf = errors.New("cannot remove the leader node; transfer leadership first")

	// ErrNodeNotInCluster is returned when trying to remove a node that is not a cluster member.
	ErrNodeNotInCluster = errors.New("node is not a member of the cluster")

	// ErrNodeSyncing is returned by ReadIndexAndWait when the node is still catching up
	// (restoring a snapshot or replaying spool). Callers should forward the read to the leader.
	ErrNodeSyncing = errors.New("node is syncing")
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

// readyResult is sent from processReadies to orchestrate after a Ready has been
// persisted. It carries deferred rawNode operations that must execute in the
// orchestrate goroutine (rawNode is not thread-safe).
type readyResult struct {
	rd raft.Ready
	// snapshotApplied is true when a leader snapshot was installed; orchestrate
	// must call rawNode.ReportSnapshot.
	snapshotApplied bool
	// confChanges are committed ConfChangeV2 entries extracted from rd.CommittedEntries;
	// orchestrate must call rawNode.ApplyConfChange for each.
	confChanges []raftpb.ConfChangeV2
}

// Node wraps raft.RawNode to provide an Apply() method similar to hashicorp/raft.
type Node struct {
	rawNode          *raft.RawNode
	logger           logging.Logger
	fsm              *state.Machine
	recovery         *state.Recovery
	synchronizer     *state.Synchronizer
	wal              wal.WAL
	transport        Transport
	config           NodeConfig
	proposeCh        chan *Proposal
	clusterCommandCh chan *clusterCommand
	confState        atomic.Pointer[raftpb.ConfState]
	lastSoftState    atomic.Pointer[raft.SoftState]
	// lastObservedTerm holds the highest Raft term this node has seen via
	// Ready (sourced from rd.HardState.Term, not rd.SoftState — SoftState has
	// no Term field). It is initialized from wal.InitialState() and updated
	// monotonically in processReady. Propose reads it atomically to tag each
	// pending FSM future, so a later term advance can fail futures whose
	// entries were truncated by the new leader (issue #172).
	lastObservedTerm atomic.Uint64
	observer         *Observer
	applier          *Applier

	readies            chan raft.Ready
	readyTerminated    chan readyResult
	tasks              *taskSet
	stopChannel        chan chan struct{}
	runDone            chan struct{} // closed when Run() exits
	recoveredPeers     map[uint64]ConfChangeContext
	peerAddressesMu    sync.RWMutex
	peerAddresses      map[uint64]ConfChangeContext
	pendingConfChanges SyncMap[uint64, *futures.Future[struct{}]]

	// confChangeMu serializes external ConfChange operations (AddLearner,
	// RemoveNode, PromoteLearner) so that only one proposal is in-flight at a
	// time. This avoids unnecessary retries caused by etcd/raft silently
	// dropping concurrent ConfChange proposals.
	confChangeMu sync.Mutex

	// lastAutoPromote tracks the last time an auto-promotion was proposed for
	// each learner node. Used to rate-limit proposals and avoid spamming raft
	// when another ConfChange is pending. Accessed only from the orchestrate loop.
	lastAutoPromote map[uint64]time.Time

	// pendingReads tracks in-flight ReadIndex requests, keyed by unique request ID.
	pendingReads *SyncMap[uint64, *readIndexRequest]

	// indexTracker provides an accurate prediction of the next Raft index.
	// Incremented by Propose() (all proposals) and processReady() (non-proposal
	// committed entries like no-ops and config changes).
	indexTracker *IndexTracker

	// leaderReady is closed (done) when the new leader's FSM has caught up
	// with all committed entries from the previous term. Admission blocks
	// on this channel before pre-reads. On leadership gain it is replaced
	// with a fresh channel; a background goroutine closes it after
	// WaitForApplied reaches the index of the leader's no-op blank entry —
	// once that is applied, every preceding log entry is committed and
	// applied too, including anything the prior leader had applied.
	leaderReady atomic.Pointer[chan struct{}]

	// lastCheckpointPersistedIndex tracks the persisted index at the time of the
	// last background Pebble checkpoint. doMaintenance skips checkpoint creation
	// when no new entries have been persisted since the previous checkpoint,
	// avoiding unnecessary I/O on the data volume.
	lastCheckpointPersistedIndex uint64

	// Metrics (kept on Node: WAL/transport/orchestrate-related)
	processEntryHistogram             metric.Int64Histogram
	appendEntriesHistogram            metric.Int64Histogram
	leadMonitorHistogram              metric.Int64Gauge
	committedEntriesPerReadyHistogram metric.Int64Histogram
	readyWaitDurationHistogram        metric.Int64Histogram
	readyTerminatedWaitHistogram      metric.Int64Histogram
	readIndexDurationHistogram        metric.Int64Histogram
}

// NewNode creates a new wrapper around a RawNode.
func NewNode(
	cfg NodeConfig,
	transport Transport,
	applier *Applier,
	logger logging.Logger,
	meter metric.Meter,
	wal wal.WAL,
	fsm *state.Machine,
	recovery *state.Recovery,
	synchronizer *state.Synchronizer,
) (*Node, error) {
	cfg.SetDefaults()

	snapshot, err := wal.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	var (
		initialConfState raftpb.ConfState
		recoveredPeers   map[uint64]ConfChangeContext
	)

	if len(snapshot.Metadata.ConfState.Voters) == 0 {
		logger.Infof("Fresh start: WAL has no ConfState voters, creating initial snapshot")

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

			if err := recovery.RecoverState(); err != nil {
				return nil, fmt.Errorf("recovering FSM state from store: %w", err)
			}

			// Wrap with empty NodeSnapshot (no FSM data, no peer addresses on restore bootstrap)
			ns := &raftcmdpb.NodeSnapshot{}

			data, err := ns.MarshalVT()
			if err != nil {
				return nil, fmt.Errorf("wrapping restore snapshot: %w", err)
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

			switch {
			case cfg.Bootstrap:
				// Bootstrap mode: this node + any known peers start as voters.
				voters = make([]uint64, 0, len(cfg.Peers)+1)

				voters = append(voters, cfg.NodeID)
				for _, peerEntry := range cfg.Peers {
					voters = append(voters, peerEntry.ID)
				}

				logger.WithFields(map[string]any{
					"voters": voters,
				}).Infof("Bootstrap mode: initializing as voter cluster")
			case len(cfg.Peers) > 0:
				// Join mode: existing peers are voters, self joins as learner.
				// The leader will add us via ConfChange after we start.
				voters = make([]uint64, 0, len(cfg.Peers))
				for _, peerEntry := range cfg.Peers {
					voters = append(voters, peerEntry.ID)
				}

				learners = []uint64{cfg.NodeID}
				logger.WithFields(map[string]any{
					"voters":   voters,
					"learners": learners,
				}).Infof("Join mode: initializing as learner, peers are voters")
			default:
				return nil, errors.New("first start requires --bootstrap or --join")
			}

			// Wrap with empty NodeSnapshot (no FSM data, no peer addresses on initial bootstrap)
			ns := &raftcmdpb.NodeSnapshot{}

			data, err := ns.MarshalVT()
			if err != nil {
				return nil, fmt.Errorf("wrapping initial snapshot: %w", err)
			}

			initialConfState = raftpb.ConfState{
				Voters:   voters,
				Learners: learners,
			}
			if err := wal.CreateSnapshot(0, &initialConfState, data); err != nil {
				return nil, fmt.Errorf("creating initial snapshot: %w", err)
			}

			// On bootstrap the node is, by definition, accepted by the
			// (single-voter) cluster the moment its initial snapshot is
			// persisted. Drop the CLUSTER_JOINED marker so the operator's
			// StatefulSet entrypoint treats subsequent restarts as pure
			// restarts. Joining nodes (--join path) write the marker
			// later, after tryAddLearner succeeds — see
			// internal/bootstrap/module.go.
			if cfg.Bootstrap {
				if err := wal.MarkClusterJoined(); err != nil {
					return nil, fmt.Errorf("marking cluster joined after bootstrap: %w", err)
				}
			}
		}
	} else {
		logger.WithFields(map[string]any{
			"snapshotIndex": snapshot.Metadata.Index,
			"snapshotTerm":  snapshot.Metadata.Term,
			"voters":        snapshot.Metadata.ConfState.Voters,
			"learners":      snapshot.Metadata.ConfState.Learners,
			"nodeID":        cfg.NodeID,
			"bootstrap":     cfg.Bootstrap,
			"peerCount":     len(cfg.Peers),
		}).Infof("Restart detected: WAL already has ConfState (not a fresh start)")

		switch {
		case snapshot.Metadata.Index > 0 && len(snapshot.Data) > 0:
			logger.WithFields(map[string]any{
				"index":        snapshot.Metadata.Index,
				"snapshotSize": len(snapshot.Data),
			}).Infof("Restoring Machine from snapshot")

			// Unwrap NodeSnapshot to get FSM data and peer addresses
			unwrapStart := time.Now()

			result, err := unwrapSnapshot(snapshot.Data)
			if err != nil {
				return nil, fmt.Errorf("unwrapping snapshot: %w", err)
			}

			logger.WithFields(map[string]any{
				"duration":  time.Since(unwrapStart).String(),
				"peerCount": len(result.peerAddresses),
			}).Infof("Unwrapped NodeSnapshot")

			if err := synchronizer.InstallSnapshot(context.Background(), snapshot); err != nil {
				panic(err)
			}

			logger.Infof("Installed FSM snapshot (snapshotIndex set, cache reset)")

			// Seed recovered peers from snapshot peer addresses
			recoveredPeers = make(map[uint64]ConfChangeContext, len(result.peerAddresses))
			for _, addr := range result.peerAddresses {
				recoveredPeers[addr.GetNodeId()] = ConfChangeContext{
					RaftAddress:    addr.GetRaftAddress(),
					ServiceAddress: addr.GetServiceAddress(),
				}
			}

			logger.Infof("Snapshot restored successfully")
		case snapshot.Metadata.Index > 0:
			// Recovered snapshot from WAL records (snap file was lost).
			// ConfState is available but peer addresses are not — they will be
			// rediscovered via etcd. The FSM state lives in Pebble (separate
			// volume), so no data resync is needed.
			logger.WithFields(map[string]any{
				"index": snapshot.Metadata.Index,
			}).Errorf("Snapshot at index %d has no peer addresses (snap file was lost); "+
				"peers will be rediscovered via etcd",
				snapshot.Metadata.Index)

			if err := synchronizer.InstallSnapshot(context.Background(), snapshot); err != nil {
				panic(err)
			}
		default:
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

		// Overlay WAL entries which may contain more recent peer addresses.
		// Scan in bounded chunks to avoid loading the entire WAL into memory.
		if recoveredPeers == nil {
			recoveredPeers = make(map[uint64]ConfChangeContext)
		}

		walScanStart := time.Now()
		firstIdx, firstErr := wal.FirstIndex()

		// Todo: is it safe if the wal was compacted?
		lastIdx, lastErr := wal.LastIndex()
		if firstErr == nil && lastErr == nil && firstIdx <= lastIdx {
			logger.WithFields(map[string]any{
				"firstIndex": firstIdx,
				"lastIndex":  lastIdx,
				"entryCount": lastIdx - firstIdx + 1,
			}).Infof("Scanning WAL entries for peer addresses")

			const maxChunkBytes = 8 * 1024 * 1024 // 8MB per chunk

			lo := firstIdx
			for lo <= lastIdx {
				entries, err := wal.Entries(lo, lastIdx+1, maxChunkBytes)
				if err != nil || len(entries) == 0 {
					break
				}

				maps.Copy(recoveredPeers, ExtractPeerAddressesFromEntries(entries))

				lo = entries[len(entries)-1].Index + 1
			}
		}

		logger.WithFields(map[string]any{
			"duration":  time.Since(walScanStart).String(),
			"peerCount": len(recoveredPeers),
		}).Infof("WAL peer address scan complete")
	}

	node := &Node{
		logger:           logger,
		wal:              wal,
		transport:        transport,
		config:           cfg,
		proposeCh:        make(chan *Proposal, cfg.ProposeQueueCapacity),
		clusterCommandCh: make(chan *clusterCommand, 1),
		fsm:              fsm,
		recovery:         recovery,
		synchronizer:     synchronizer,
		applier:          applier,
		readies:          make(chan raft.Ready, 1),
		readyTerminated:  make(chan readyResult, 1),
		tasks:            newTaskSet(),
		stopChannel:      make(chan chan struct{}),
		pendingReads:     &SyncMap[uint64, *readIndexRequest]{},
		recoveredPeers:   recoveredPeers,
		peerAddresses:    recoveredPeers,
		lastAutoPromote:  make(map[uint64]time.Time),
		indexTracker:     NewIndexTracker(initialIndex(wal)),
		observer:         NewNoOpObserver(),
	}

	// Start with a closed channel (leader ready — no leadership transition yet).
	readyCh := make(chan struct{})
	close(readyCh)
	node.leaderReady.Store(&readyCh)

	logger.WithFields(map[string]any{
		"initialIndex": initialIndex(wal),
	}).Infof("IndexTracker initialized")

	// Seed lastObservedTerm from the persisted HardState so that any Propose
	// that lands before the first Ready already tags futures with the correct
	// term. This closes the term=0 startup window referenced by issue #172.
	if hs, _, hsErr := wal.InitialState(); hsErr == nil {
		node.lastObservedTerm.Store(hs.Term)
	}

	node.confState.Store(&initialConfState)

	// Ensure peerAddresses is never nil (bootstrap path has no recoveredPeers).
	if node.peerAddresses == nil {
		node.peerAddresses = make(map[uint64]ConfChangeContext)
	}

	// Initialize node metrics
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

	node.readIndexDurationHistogram, err = meter.Int64Histogram(
		"raft.read_index.duration",
		metric.WithDescription("Time spent in ReadIndex+WaitForApplied for linearizable reads"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000,
		),
	)
	if err != nil {
		panic(err)
	}

	storeUpToDate, err := applier.RecoverAndReplay(context.Background())
	if err != nil {
		return nil, err
	}

	if storeUpToDate {
		// Early compaction: if the WAL has accumulated far more entries than the
		// compaction margin, create a snapshot and compact now to release memory
		// before the Raft node starts. Without this, the leader would try to
		// replicate tens of thousands of entries to lagging followers, causing OOM.
		compactStart := time.Now()

		if err := node.maybeCompactAtStartup(context.Background()); err != nil {
			return nil, fmt.Errorf("early compaction: %w", err)
		}

		if time.Since(compactStart) > 100*time.Millisecond {
			logger.WithFields(map[string]any{
				"duration": time.Since(compactStart).String(),
			}).Infof("Early compaction complete")
		}
	}

	return node, nil
}

// maybeCompactAtStartup creates a snapshot and compacts the WAL if entries have
// accumulated beyond the compaction margin. This prevents OOM during Raft
// replication: without compaction a leader with 50K+ WAL entries would try to
// send them all to lagging followers, exhausting memory.
func (node *Node) maybeCompactAtStartup(ctx context.Context) error {
	lastSnap, err := node.wal.Snapshot()
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}

	walLastIdx, err := node.wal.LastIndex()
	if err != nil {
		return fmt.Errorf("reading WAL last index: %w", err)
	}

	if walLastIdx <= lastSnap.Metadata.Index+node.applier.CompactionMargin() {
		return nil
	}

	appliedIndex, err := query.ReadLastAppliedIndex(node.applier.Store())
	if err != nil {
		return fmt.Errorf("reading applied index: %w", err)
	}

	// Only snapshot at the applied index (not walLastIdx) — the FSM may not
	// have processed every WAL entry yet.
	if appliedIndex <= lastSnap.Metadata.Index {
		return nil
	}

	node.logger.WithFields(map[string]any{
		"lastSnapshotIndex":  lastSnap.Metadata.Index,
		"walLastIndex":       walLastIdx,
		"appliedIndex":       appliedIndex,
		"entriesAccumulated": walLastIdx - lastSnap.Metadata.Index,
	}).Infof("WAL has excess entries, compacting before Raft start")

	compactionStart := time.Now()

	// Wrap nil FSM data with peer addresses for the WAL snapshot.
	snapshotData, err := node.wrapSnapshot()
	if err != nil {
		return fmt.Errorf("wrapping snapshot: %w", err)
	}

	if err := node.wal.CreateSnapshot(appliedIndex, node.confState.Load(), snapshotData); err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	node.logger.WithFields(map[string]any{
		"snapshotSize": len(snapshotData),
		"appliedIndex": appliedIndex,
	}).Infof("Saved snapshot to WAL")

	if appliedIndex > node.applier.CompactionMargin() {
		compactIdx := appliedIndex - node.applier.CompactionMargin()
		compactStart := time.Now()

		err := node.wal.Compact(compactIdx)
		if err != nil && !errors.Is(err, raft.ErrCompacted) {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Early compaction failed")
		} else {
			node.logger.WithFields(map[string]any{
				"duration":     time.Since(compactStart).String(),
				"compactIndex": compactIdx,
			}).Infof("WAL compacted")
		}
	}

	node.logger.WithFields(map[string]any{
		"totalDuration": time.Since(compactionStart).String(),
	}).Infof("Early compaction completed")

	return nil
}

// runBackgroundMaintenance periodically creates WAL snapshots, compacts the WAL,
// and creates Pebble checkpoints. This replaces both the old triggerSnapshot
// mechanism (which used gating in the applier) and Store.RunBackgroundCheckpoints.
func (node *Node) runBackgroundMaintenance(ctx context.Context, stop chan struct{}) error {
	interval := node.config.MaintenanceInterval

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return nil
		case <-ticker.C:
			node.doMaintenance()
		}
	}
}

func (node *Node) doMaintenance() {
	store := node.applier.Store()

	lastSnap, err := node.wal.Snapshot()
	if err != nil {
		node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to read WAL snapshot")

		return
	}

	// Early skip: nothing new since the previous tick. Avoids paying for an
	// fsync on idle clusters.
	if node.fsm.LastPersistedIndex() <= lastSnap.Metadata.Index {
		return
	}

	// Capture lastPersistedIndex BEFORE calling SyncWAL. Reading first then
	// syncing guarantees the captured index is covered by the fsync: at the
	// moment we read N from lastPersistedIndex the corresponding batch has
	// already been written to Pebble's WAL (FSM publishes the index after
	// batch.Commit returns, see machine.go), so the subsequent SyncWAL makes
	// it durable. The reverse order (sync, then read) could capture an index
	// from a concurrent apply whose WAL record was written after our fsync.
	capturedIndex := node.fsm.LastPersistedIndex()
	if err := store.SyncWAL(); err != nil {
		node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to sync Pebble WAL, skipping snapshot and checkpoint")

		return
	}
	// Post-condition: every FSM batch with Raft index <= capturedIndex is
	// durable on disk. It is now safe to advance the Raft WAL snapshot and
	// to compact the Raft WAL up to capturedIndex - margin — even on power
	// loss right after this point, Pebble can recover to >= capturedIndex.

	// 1. WAL snapshot + compact.
	data, err := node.wrapSnapshot()
	if err != nil {
		node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to wrap snapshot")
	} else if err := node.wal.CreateSnapshot(capturedIndex, node.confState.Load(), data); err != nil {
		if !errors.Is(err, raft.ErrSnapOutOfDate) {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to create WAL snapshot")
		}
	} else if capturedIndex > node.applier.CompactionMargin() {
		compactIdx := capturedIndex - node.applier.CompactionMargin()
		if err := node.wal.Compact(compactIdx); err != nil && !errors.Is(err, raft.ErrCompacted) {
			node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to compact WAL")
		}
	}

	// 2. Pebble checkpoint (for cold-restart DR — operator k8s bootstrap detection).
	// Independent of step 1: failures in the WAL snapshot/compact path must not
	// prevent the Pebble checkpoint from being attempted, and vice versa.
	if capturedIndex <= node.lastCheckpointPersistedIndex {
		return
	}

	if _, err := store.CreateSnapshot(); err != nil {
		node.logger.WithFields(map[string]any{"error": err}).Errorf("Background maintenance: failed to create Pebble checkpoint")

		return
	}

	node.lastCheckpointPersistedIndex = capturedIndex
}

func (node *Node) Run(ctx context.Context, ready chan struct{}) error {
	node.runDone = make(chan struct{})
	defer close(node.runDone)

	// Determine the Applied index for raft.Config from the FSM's durable last
	// applied index (read from Pebble). doMaintenance calls Store.SyncWAL
	// before creating each Raft WAL snapshot, so under correct operation the
	// Raft WAL snapshot's Metadata.Index is always <= Pebble's durable
	// applied index. The check below is defense-in-depth: it catches a
	// pathological state (SyncWAL silently regressed, manual snapshot
	// creation outside doMaintenance, Pebble bug) before Raft starts, rather
	// than letting it manifest as silent data loss.
	applied := node.fsm.LastAppliedIndex()

	walSnap, err := node.wal.Snapshot()
	if err != nil {
		return fmt.Errorf("reading WAL snapshot for Applied: %w", err)
	}

	walFirstIdx, err := node.wal.FirstIndex()
	if err != nil {
		return fmt.Errorf("reading WAL first index: %w", err)
	}

	// If Pebble's durable applied index is below the WAL's first available
	// entry, the entries needed to catch the FSM up to walSnap.Metadata.Index
	// are gone from both Pebble and the Raft WAL. With Store.SyncWAL in the
	// maintenance path this is unreachable in correct operation; if it
	// triggers, something is very wrong — refuse to start rather than mask it.
	if applied+1 < walFirstIdx {
		return fmt.Errorf(
			"durability gap exceeds WAL retention: Pebble applied=%d, WAL firstIndex=%d, "+
				"WAL snapshot=%d. The compaction margin was overrun before Pebble fsync'd. "+
				"Restore from a Pebble checkpoint or contact ops",
			applied, walFirstIdx, walSnap.Metadata.Index,
		)
	}

	if walSnap.Metadata.Index > applied {
		// Should not happen with SyncWAL in doMaintenance; log loudly so the
		// regression is visible. Raft can still recover via redelivery of
		// [Applied+1, Commit] in CommittedEntries.
		node.logger.WithFields(map[string]any{
			"storeApplied":   applied,
			"walSnapshotIdx": walSnap.Metadata.Index,
		}).Errorf("Pebble lags WAL snapshot — SyncWAL durability invariant violated, relying on Raft replay")
	}

	// Cap applied to the WAL's durable commit index. etcd's WAL skips fsync
	// for commit-only HardState updates (MustSync checks entries/term/vote,
	// not commit). A crash can lose uncommitted commit advances, leaving
	// Pebble's lastAppliedIndex ahead of the WAL's HardState.Commit. Raft
	// panics at startup if applied > committed. Capping here is safe because
	// the FSM's ApplyEntries skips entries whose index <= lastAppliedIndex
	// (loaded from Pebble), so re-delivered entries are no-ops.
	hardState, _, err := node.wal.InitialState()
	if err != nil {
		return fmt.Errorf("reading WAL initial state for Applied: %w", err)
	}

	if hardState.Commit < applied {
		node.logger.WithFields(map[string]any{
			"storeApplied":     applied,
			"walDurableCommit": hardState.Commit,
		}).Infof("Pebble applied ahead of WAL durable commit, capping Applied")

		applied = hardState.Commit
	}

	// Initialize lastCheckpointPersistedIndex from the durable applied index.
	// Treating Applied as "at least as recent as the checkpoint we would
	// create now" prevents a redundant Pebble checkpoint on the very first
	// maintenance tick after restart.
	node.lastCheckpointPersistedIndex = applied

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
		// Tell raft which entries the FSM has already applied so that the first
		// Ready does not re-emit them in CommittedEntries. Without this, the
		// IndexTracker double-counts non-proposal WAL entries (ConfChange, no-ops)
		// because they are already accounted for by initialIndex(wal).
		Applied: applied,
	}

	node.logger.WithFields(map[string]any{
		"id":              raftConfig.ID,
		"electionTick":    raftConfig.ElectionTick,
		"heartbeatTick":   raftConfig.HeartbeatTick,
		"maxSizePerMsg":   raftConfig.MaxSizePerMsg,
		"maxInflightMsgs": raftConfig.MaxInflightMsgs,
		"preVote":         raftConfig.PreVote,
	}).Infof("Starting raft node")

	node.rawNode, err = raft.NewRawNode(raftConfig)
	if err != nil {
		return fmt.Errorf("creating raw rawNode: %w", err)
	}

	status := node.rawNode.Status()

	// Log initial raft state for cluster join diagnostics
	node.logger.WithFields(map[string]any{
		"nodeID":    node.config.NodeID,
		"raftState": status.RaftState.String(),
		"lead":      status.Lead,
		"term":      status.HardState.Term,
		"commit":    status.HardState.Commit,
		"vote":      status.HardState.Vote,
		"voters":    node.confState.Load().Voters,
		"learners":  node.confState.Load().Learners,
	}).Infof("Raft node created — initial state")

	node.tasks.add(newTask(node.orchestrate))
	node.tasks.add(newTask(node.applier.Run))
	node.tasks.add(newTask(node.processReadies))
	node.tasks.add(newTask(node.runBackgroundMaintenance))
	node.tasks.run(ctx)

	// Wait for the FSM to apply all initially committed entries before
	// signaling ready. This ensures downstream consumers (index builder,
	// event manager) see the complete Pebble state at startup.
	initialCommit := status.Commit
	if initialCommit > 0 {
		node.logger.WithFields(map[string]any{
			"from": node.fsm.LastPersistedIndex(),
			"to":   initialCommit,
		}).Infof("Replaying WAL...")

		err := node.fsm.WaitForApplied(ctx, initialCommit)
		if err != nil {
			return fmt.Errorf("waiting for initial WAL replay: %w", err)
		}

		node.logger.WithFields(map[string]any{
			"appliedUpTo": initialCommit,
		}).Infof("Initial WAL replay complete")
	}

	close(ready)

	select {
	case ch := <-node.stopChannel:
		err := node.tasks.stop()
		if err != nil {
			node.logger.Errorf("Error stopping task pool: %v", err)
		}

		// Stop background bloom tasks that may hold Pebble iterators.
		// Must run after tasks.stop() (which stops the applier that can
		// trigger new bloom tasks) and before the fx hook closes the DB.
		node.fsm.StopBackgroundTasks()

		close(ch)

		return nil
	case err := <-node.tasks.err():
		stopErr := node.tasks.stop()
		if stopErr != nil {
			node.logger.Errorf("Error stopping remaining tasks after failure: %v", stopErr)
		}

		node.fsm.StopBackgroundTasks()

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
			result, err := node.processReady(ctx, stop, rd)
			node.processEntryHistogram.Record(context.Background(), time.Since(now).Microseconds())

			if err != nil {
				return err
			}

			terminatedStart := time.Now()

			select {
			case node.readyTerminated <- result:
				node.readyTerminatedWaitHistogram.Record(context.Background(), time.Since(terminatedStart).Microseconds())
			case <-stop:
				return nil
			}
		case <-stop:
			return nil
		}
	}
}

func (node *Node) processReady(ctx context.Context, stop chan struct{}, rd raft.Ready) (readyResult, error) {
	if node.logger.Enabled(logging.TraceLevel) {
		node.logger.Tracef("Processing ready")
	}

	node.committedEntriesPerReadyHistogram.Record(context.Background(), int64(len(rd.CommittedEntries)))

	if rd.SoftState != nil {
		ss := rd.SoftState
		// Only trigger sync from SoftState if this Ready does NOT also contain
		// a snapshot. When both are present, the snapshot processing below will
		// trigger its own syncSnapshot. Doing it here too would start a background
		// task that is immediately interrupted by the second syncSnapshot call,
		// which corrupts the spool read cache (entries read but never applied).
		if ss.Lead != 0 && node.applier.Status() == statusOutOfSync && raft.IsEmptySnap(rd.Snapshot) && !isStopping(stop) {
			node.applier.SyncSnapshot(ss.Lead, stop)
		}

		actualNodeLastSoftState := node.lastSoftState.Load()
		wasLeader := actualNodeLastSoftState != nil && actualNodeLastSoftState.RaftState == raft.StateLeader
		isLeader := ss.RaftState == raft.StateLeader

		// Fail pending ReadIndex requests whenever the leader changes.
		// They were dispatched to the old leader and won't be answered.
		var previousLead uint64
		if actualNodeLastSoftState != nil {
			previousLead = actualNodeLastSoftState.Lead
		}

		if previousLead != ss.Lead {
			node.failAllPendingReads(ErrNotLeader)
		}

		if wasLeader != isLeader {
			// Use rd.HardState.Term instead of rawNode.Status().Term to avoid
			// calling rawNode from the processReadies goroutine (rawNode is not thread-safe).
			term := rd.Term
			logger := node.logger.WithFields(map[string]any{
				"lead": ss.Lead,
				"term": term,
			})

			// leadership loss
			if wasLeader && !isLeader {
				logger.Infof("Leadership lost")
				details := map[string]any{
					"nodeID": node.config.NodeID,
					"lead":   ss.Lead,
					"term":   term,
				}
				lifecycle.SendEvent("leadership_lost", details)
				node.observer.Emit(LeadershipChangeEvent{IsLeader: false})
			}
			// acquire leadership
			if !wasLeader && isLeader {
				logger.Infof("Leadership gained")
				details := map[string]any{
					"nodeID": node.config.NodeID,
					"lead":   ss.Lead,
					"term":   term,
				}
				assert.Sometimes(true, "leadership gained", details)
				lifecycle.SendEvent("leadership_gained", details)
				node.observer.Emit(LeadershipChangeEvent{IsLeader: true})

				// Block admission until the FSM has applied all entries the
				// previous leader had applied. The target is the no-op blank
				// entry appended by raft on leadership gain (last of
				// rd.Entries): once it's committed (majority replication) and
				// applied, every preceding log entry is committed too (leader
				// completeness + the rule that commits always pass through a
				// current-term entry), including entries the old leader
				// applied whose commit broadcast never reached us.
				target, err := leadershipGainTarget(rd)
				if err != nil {
					emptyDetails := map[string]any{
						"nodeID": node.config.NodeID,
						"lead":   ss.Lead,
						"term":   term,
					}
					node.logger.WithFields(emptyDetails).Errorf("Leadership gained Ready has no entries — etcd/raft contract changed")
					assert.Unreachable("leadership gain Ready must carry the no-op blank entry", emptyDetails)
					lifecycle.SendEvent("leadership_gain_no_entries", emptyDetails)

					return readyResult{}, fmt.Errorf("leadership gain (nodeID=%d term=%d): %w",
						node.config.NodeID, term, err)
				}

				pending := make(chan struct{})
				node.leaderReady.Store(&pending)

				node.applier.Drain(stop)
				node.recovery.OnLeadershipAcquired(stop)

				go func() {
					if err := node.fsm.WaitForApplied(ctx, target); err != nil {
						node.logger.WithFields(map[string]any{
							"error":  err,
							"target": target,
						}).Errorf("Failed to wait for FSM catch-up after leadership gain")
					}

					node.observer.Emit(LeaderReadyEvent{})

					close(pending)
				}()
			}
		}

		node.leadMonitorHistogram.Record(ctx, int64(ss.Lead))

		node.lastSoftState.Store(ss)
	}

	// Resolve pending ReadIndex requests from rd.ReadStates.
	for _, rs := range rd.ReadStates {
		reqID, ok := parseReadIndexContext(rs.RequestCtx)
		if !ok {
			continue
		}

		req, loaded := node.pendingReads.Load(reqID)
		if !loaded {
			continue
		}

		node.pendingReads.Delete(reqID)
		req.future.Resolve(rs.Index, nil)
	}

	now := time.Now()

	err := node.wal.Append(rd.HardState, rd.Entries)
	if err != nil {
		return readyResult{}, fmt.Errorf("appending entries to storage: %w", err)
	}

	node.appendEntriesHistogram.Record(ctx, time.Since(now).Microseconds())

	// Track the highest term observed via HardState so Propose can tag each
	// future with the proposer's view of the current term (issue #172). Use a
	// monotonic CAS — terms only grow.
	if !raft.IsEmptyHardState(rd.HardState) {
		for {
			cur := node.lastObservedTerm.Load()
			if cur >= rd.Term {
				break
			}

			if node.lastObservedTerm.CompareAndSwap(cur, rd.Term) {
				break
			}
		}
	}

	result := readyResult{rd: rd}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapshotStart := time.Now()

		snapshotDetails := map[string]any{
			"nodeID":       node.config.NodeID,
			"index":        rd.Snapshot.Metadata.Index,
			"snapshotSize": len(rd.Snapshot.Data),
		}
		node.logger.WithFields(snapshotDetails).Infof("Applying snapshot sent by leader")
		lifecycle.SendEvent("snapshot_received", snapshotDetails)

		// Ask the Run goroutine to drain all pending work, interrupt any
		// running maintenance task, and set statusInstallingSnapshot. This
		// is the single-writer protocol: only Run writes status, so there
		// is no race between status writes from processReadies and entry
		// processing in Run (which could trigger handleCheckpointRequired).
		//
		// After PrepareForSnapshotInstall returns, Run is idle and the
		// status is statusInstallingSnapshot — safe to write FSM fields.
		node.applier.PrepareForSnapshotInstall(stop)

		if err := node.wal.ApplySnapshot(rd.Snapshot); err != nil {
			return readyResult{}, fmt.Errorf("applying snapshot to storage: %w", err)
		}

		// Unwrap NodeSnapshot to get FSM data and peer addresses
		unwrapStart := time.Now()

		snapResult, err := unwrapSnapshot(rd.Snapshot.Data)
		if err != nil {
			return readyResult{}, fmt.Errorf("unwrapping snapshot: %w", err)
		}

		node.logger.WithFields(map[string]any{
			"duration":  time.Since(unwrapStart).String(),
			"peerCount": len(snapResult.peerAddresses),
		}).Infof("Unwrapped leader snapshot")

		if err := node.synchronizer.InstallSnapshot(ctx, rd.Snapshot); err != nil {
			return readyResult{}, fmt.Errorf("installing snapshot: %w", err)
		}

		// Restore peer addresses into node
		node.peerAddressesMu.Lock()
		node.peerAddresses = make(map[uint64]ConfChangeContext, len(snapResult.peerAddresses))
		for _, addr := range snapResult.peerAddresses {
			node.peerAddresses[addr.GetNodeId()] = ConfChangeContext{
				RaftAddress:    addr.GetRaftAddress(),
				ServiceAddress: addr.GetServiceAddress(),
			}
		}
		node.peerAddressesMu.Unlock()

		// Defer ReportSnapshot to orchestrate goroutine (rawNode is not thread-safe).
		result.snapshotApplied = true

		node.logger.WithFields(map[string]any{
			"duration": time.Since(snapshotStart).String(),
			"index":    rd.Snapshot.Metadata.Index,
		}).Infof("Snapshot from leader applied, starting checkpoint sync")

		// The snapshot is already persisted in WAL at this point. If syncSnapshot
		// fails (network issue, leader unavailable, etc.), the node transitions to
		// statusOutOfSync and will retry automatically when a leader is detected
		// via SoftState or on restart (isStoreUpToDate check).
		// Skip sync if the node is shutting down — RestoreCheckpoint reopens the
		// Pebble DB, and background tasks (bloom restore) would create iterators
		// that outlive the DB.Close() in the fx shutdown hook.
		if !isStopping(stop) {
			node.applier.SyncSnapshot(node.lastSoftState.Load().Lead, stop)
		}
	}

	node.transport.Send(rd.Messages)

	// Extract conf changes from committed entries. The actual rawNode.ApplyConfChange
	// calls are deferred to the orchestrate goroutine (rawNode is not thread-safe).
	for _, entry := range rd.CommittedEntries {
		cc, ok, err := unmarshalConfChangeV2(entry)
		if err != nil {
			return readyResult{}, err
		}

		if ok {
			result.confChanges = append(result.confChanges, cc)
		}
	}

	return result, nil
}

// finishReady completes processing of a Ready by applying deferred rawNode operations
// and post-processing. Must be called from the orchestrate goroutine.
func (node *Node) finishReady(result readyResult, stop chan struct{}) error {
	rd := result.rd

	if result.snapshotApplied {
		node.rawNode.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)

		// Re-sync the in-memory ConfState shadow from the just-installed
		// snapshot. wal.ApplySnapshot already persisted the correct ConfState;
		// without this, the reconcile block below loads the stale shadow and
		// overwrites the WAL with it (EN-1278). Fresh pointer copy avoids
		// aliasing the Ready's snapshot struct. Runs before the conf-change
		// loop so a combined snapshot+conf-change Ready still layers the delta
		// on top of the correct baseline.
		cs := rd.Snapshot.Metadata.ConfState
		node.confState.Store(&cs)
	}

	// Apply conf changes (rawNode.ApplyConfChange must run in orchestrate goroutine).
	// Collect pending futures to resolve AFTER the WAL ConfState update, so
	// callers waiting on ConfChange commit (AddLearner, PromoteLearner, etc.)
	// don't resume before the WAL is consistent.
	var pendingFutures []*futures.Future[struct{}]

	for _, cc := range result.confChanges {
		node.logger.
			WithFields(map[string]any{"transition": cc.Transition.String()}).
			Infof("Applying configuration change")
		node.confState.Store(node.rawNode.ApplyConfChange(cc))

		// Update peer address map and collect pending futures
		for _, change := range cc.Changes {
			switch change.Type {
			case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
				if len(cc.Context) > 0 {
					if ccCtx, err := UnmarshalConfChangeContext(cc.Context); err == nil {
						node.SetPeerAddress(change.NodeID, ccCtx.RaftAddress, ccCtx.ServiceAddress)
					}
				}
			case raftpb.ConfChangeRemoveNode:
				node.RemovePeerAddress(change.NodeID)
			}

			// Collect pending ConfChange future (if any) — resolved below after WAL update.
			if f, ok := node.pendingConfChanges.LoadAndDelete(change.NodeID); ok {
				pendingFutures = append(pendingFutures, f)
			}
		}

		// Notify observers about configuration changes
		for _, change := range cc.Changes {
			lifecycle.SendEvent("conf_change_committed", map[string]any{
				"nodeID":     node.config.NodeID,
				"targetNode": change.NodeID,
				"changeType": change.Type.String(),
			})
			node.observer.Emit(ConfChangeEvent{
				NodeID:     change.NodeID,
				ChangeType: change.Type,
				Context:    cc.Context,
			})
		}
	}

	// If the ConfState changed (e.g. a learner was added), update the WAL
	// snapshot's ConfState immediately. Without this, etcd/raft would send
	// the stale snapshot (which lacks the new node) before the applier's
	// async snapshot creation finishes, causing the new node to reject it.
	if cs := node.confState.Load(); cs != nil {
		snap, _ := node.wal.Snapshot()
		if !confStatesEqual(cs, &snap.Metadata.ConfState) {
			err := node.wal.UpdateSnapshotConfState(cs)
			if err != nil {
				return fmt.Errorf("updating snapshot confstate: %w", err)
			}
		}
	}

	// Resolve pending ConfChange futures now that WAL is consistent.
	for _, f := range pendingFutures {
		f.Resolve(struct{}{}, nil)
	}

	// Update the IndexTracker to reflect the latest committed index.
	//
	// When the node is the leader, Advance (monotonically increasing) is
	// sufficient because every local log entry was counted by Increment.
	//
	// When the node is NOT the leader, the tracker may be inflated: proposals
	// that were accepted by rawNode.Propose while leader but never committed
	// (log truncated after leadership loss) leave permanent upward drift.
	// Advance cannot correct this because it only increases. Instead, use
	// Correct to force-set the tracker to lastCommitted+1. This is safe
	// because admission is only routed to the leader, so a non-leader's
	// proposeCh is always empty.
	if len(rd.CommittedEntries) > 0 {
		lastCommitted := rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		before := node.indexTracker.Next()

		ss := node.lastSoftState.Load()
		isLeader := ss != nil && ss.RaftState == raft.StateLeader

		if isLeader {
			node.indexTracker.Advance(lastCommitted + 1)
		} else {
			node.indexTracker.Correct(lastCommitted + 1)
		}

		after := node.indexTracker.Next()
		if before != after {
			node.logger.WithFields(map[string]any{
				"lastCommitted":  lastCommitted,
				"trackerBefore":  before,
				"trackerAfter":   after,
				"committedCount": len(rd.CommittedEntries),
				"isLeader":       isLeader,
			}).Infof("IndexTracker updated in finishReady")
		}
	}

	// Submit committed entries to the Applier for async FSM application
	if len(rd.CommittedEntries) > 0 {
		node.applier.Submit(rd.CommittedEntries, node.confState.Load(), stop)
	}

	return nil
}

// isStopping returns true if the stop channel has been closed or a signal is pending.
func isStopping(stop chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// shouldTickRaft returns true if the orchestrate loop should call
// rawNode.Tick() for the given applier status.
//
// Tick MUST keep firing during statusGated. The applier enters statusGated
// for CloseChapter seal checkpoints and query checkpoints — on the leader
// AND on followers. Suppressing Tick on the leader means no MsgHeartbeat
// is emitted for the entire duration of a checkpoint, and any checkpoint
// longer than the election timeout (default 1s) lets followers depose the
// leader on every chapter close. Tick only needs to be suppressed when the
// node is genuinely behind raft state (#316).
func shouldTickRaft(status int32) bool {
	switch status {
	case statusOutOfSync, statusInstallingSnapshot:
		return false
	default:
		return true
	}
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
			s := node.applier.Status()
			if msg.Type == raftpb.MsgTimeoutNow && s != statusNormal {
				node.logger.Infof("Rejecting MsgTimeoutNow while syncing")

				continue
			}

			// Diagnostic: log messages stepped while a Ready is being processed,
			// as they can mutate rawNode state before Advance is called.
			if node.readyTerminated != nil && (msg.Type == raftpb.MsgApp || msg.Type == raftpb.MsgSnap) {
				node.logger.WithFields(map[string]any{
					"type":       msg.Type.String(),
					"from":       msg.From,
					"term":       msg.Term,
					"logTerm":    msg.LogTerm,
					"index":      msg.Index,
					"commit":     msg.Commit,
					"entryCount": len(msg.Entries),
				}).Infof("Stepping MsgApp/MsgSnap while Ready in flight")
			}

			err := node.rawNode.Step(msg)
			if err != nil {
				if errors.Is(err, raft.ErrStepPeerNotFound) {
					if node.logger.Enabled(logging.TraceLevel) {
						node.logger.Tracef("Ignoring message from unknown peer %d (type=%s)", msg.From, msg.Type)
					}

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
			node.readyTerminated = make(chan readyResult, 1)

			processingTick.Stop()

			node.readies <- node.rawNode.Ready()
		}
	}

	for {
		select {
		case <-ticker.C:
			status := node.applier.Status()
			if shouldTickRaft(status) {
				node.rawNode.Tick()
			}

			// Learner promotion proposes a ConfChange; keep it strictly to
			// the normal path so we don't try to drive cluster topology
			// while a maintenance window is open.
			if status == statusNormal && node.config.AutoPromoteThreshold > 0 {
				node.checkAndPromoteLearners()
			}
		case msgs := <-node.transport.RecvHighPriority():
			err := stepMessages(msgs)
			if err != nil {
				return err
			}

			maybeCreateReady()
		case <-stop:
			node.logger.Infof("Stopping readyLoop as context was cancelled")
			node.applier.Interrupt()

			return nil
		default:
			select {
			case msgs := <-node.transport.RecvHighPriority():
				err := stepMessages(msgs)
				if err != nil {
					return err
				}

				maybeCreateReady()
			case msgs := <-node.transport.RecvMediumPriority():
				err := stepMessages(msgs)
				if err != nil {
					return err
				}

				maybeCreateReady()
			case p := <-node.proposeCh:
				node.handleProposal(p)
			case cmd := <-node.clusterCommandCh:
				cmd.errCh <- cmd.fn()
			default:
				select {
				case result := <-node.readyTerminated:
					if err := node.finishReady(result, stop); err != nil {
						return err
					}

					// Diagnostic: log the applied cursor that Advance will
					// pass to raftLog.appliedTo, so we can trace regressions.
					if len(result.rd.CommittedEntries) > 0 && node.logger.Enabled(logging.TraceLevel) {
						cursor := result.rd.CommittedEntries[len(result.rd.CommittedEntries)-1].Index
						status := node.rawNode.Status()
						node.logger.WithFields(map[string]any{
							"appliedCursor":  cursor,
							"raftApplied":    status.Applied,
							"raftCommitted":  status.Commit,
							"committedCount": len(result.rd.CommittedEntries),
							"firstCommitted": result.rd.CommittedEntries[0].Index,
							"hasSnapshot":    !raft.IsEmptySnap(result.rd.Snapshot),
							"snapshotIndex":  result.rd.Snapshot.Metadata.Index,
						}).Tracef("Pre-Advance diagnostic")
					}

					node.rawNode.Advance(result.rd)

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

					node.readyTerminated = make(chan readyResult, 1)

					processingTick.Stop()

					node.readies <- node.rawNode.Ready()
				case <-stop:
					node.logger.Infof("Stopping readyLoop as context was cancelled")
					node.applier.Interrupt()

					return nil
				case nodeID := <-node.transport.Unreachable():
					node.rawNode.ReportUnreachable(nodeID)
				case msgs := <-node.transport.RecvHighPriority():
					err := stepMessages(msgs)
					if err != nil {
						return err
					}

					maybeCreateReady()
				case msgs := <-node.transport.RecvMediumPriority():
					err := stepMessages(msgs)
					if err != nil {
						return err
					}

					maybeCreateReady()
				case msgs := <-node.transport.RecvLowPriority():
					err := stepMessages(msgs)
					if err != nil {
						return err
					}

					maybeCreateReady()
				case p := <-node.proposeCh:
					node.handleProposal(p)
				case cmd := <-node.clusterCommandCh:
					cmd.errCh <- cmd.fn()
				case err := <-node.applier.TaskError():
					return fmt.Errorf("task executor error: %w", err)
				}
			}
		}
	}
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

// TransferLeader initiates a leadership transfer to the given node.
// It dispatches the request to the orchestrate loop (since rawNode is not thread-safe)
// and then polls lastSoftState to confirm the leader has changed.
func (node *Node) TransferLeader(ctx context.Context, transferee uint64) error {
	// No-op if transferee is this node and we're already leader
	if transferee == node.config.NodeID && node.IsLeader() {
		return nil
	}

	err := node.execClusterCommand(ctx, func() error {
		return node.handleTransferLeader(transferee)
	})
	if err != nil {
		return err
	}

	// Poll lastSoftState to confirm the leader has changed
	timeout := max(time.Duration(2*node.config.ElectionTick)*node.config.TickInterval, 2*time.Second)

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
//
// Increment is performed BEFORE the channel send to prevent a race with
// finishReady's Advance: after the proposeCh rendezvous, the readyLoop can
// commit the entry and call Advance(lastCommitted+1) before the caller's
// goroutine resumes. If Increment ran after the send, both Advance and
// Increment would advance the tracker for the same entry, causing a permanent
// +1 inflation that shifts preload boundaries.
//
// The send blocks until either the proposeCh has capacity or the context is
// cancelled. This applies natural backpressure when the pipeline is full
// instead of failing fast.
const proposeTimeout = 10 * time.Millisecond

func (node *Node) Propose(ctx context.Context, proposal *Proposal) (*futures.Future[state.ApplyResult], error) {
	ctx, cancel := context.WithTimeout(ctx, proposeTimeout)
	defer cancel()

	// Create a separate future for Machine results.
	// The proposal's embedded Future is for Raft consensus (resolved by rawNode.Propose).
	// The fsmFuture is for Machine processing (resolved when entry is applied).
	fsmFuture := futures.New[state.ApplyResult]()

	// Tag the future with the term observed by this goroutine. lastObservedTerm
	// lags the actual rawNode term (it is updated only in processReady), so the
	// captured value is a lower bound on the real proposal term. The lower
	// bound is safe: FailFuturesBelowTerm only fires once a strictly higher
	// term has been APPLIED locally, which is unambiguous evidence the
	// proposal was truncated.
	proposalTerm := node.lastObservedTerm.Load()
	node.applier.StoreFuture(proposal.commandID, proposalTerm, fsmFuture)

	// Pre-increment before the channel send. All callers hold the tracker's
	// mutex, so this is serialized with other proposals and with Decrement.
	node.indexTracker.Increment(1)

	select {
	case node.proposeCh <- proposal:
		return fsmFuture, nil
	case <-ctx.Done():
		// Roll back the pre-increment. Use RollbackIncrement (no mutex)
		// because the caller already holds the tracker lock.
		node.indexTracker.RollbackIncrement(1)
		node.applier.DeleteFuture(proposal.commandID)

		return nil, ctx.Err()
	}
}

// handleProposal sends a proposal to rawNode and rolls back the IndexTracker
// if Raft drops the proposal (e.g. the node is no longer leader).
// Must be called from the readyLoop goroutine.
func (node *Node) handleProposal(p *Proposal) {
	err := node.rawNode.Propose(p.data)
	if err != nil {
		// The IndexTracker was optimistically incremented in Propose().
		// Since Raft rejected this proposal, no log entry was created—
		// decrement to keep the tracker accurate.
		prev := node.indexTracker.Next()
		node.indexTracker.Decrement(1)
		node.logger.WithFields(map[string]any{
			"error":         err,
			"trackerBefore": prev,
			"trackerAfter":  node.indexTracker.Next(),
		}).Errorf("Proposal dropped, IndexTracker decremented")

		// The FSM future stored at Propose time would otherwise leak until
		// a later term advance reclaims it via FailFuturesBelowTerm. Resolve
		// it immediately so the caller's fsmFuture.Wait() unblocks with the
		// drop error.
		node.applier.ResolveDroppedFuture(p.commandID, err)
	}
	p.Resolve(nil, err)
}

// IndexTracker returns the shared index tracker for accurate Raft index prediction.
func (node *Node) IndexTracker() *IndexTracker {
	return node.indexTracker
}

// InitialIndex returns the next Raft index at node creation time.
//
// Deprecated: use IndexTracker().Next() instead.
func (node *Node) InitialIndex() uint64 {
	return initialIndex(node.wal)
}

// initialIndex computes the first index that will be assigned by Raft.
func initialIndex(w wal.WAL) uint64 {
	ret, err := w.LastIndex()
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

func (node *Node) GetLeader() uint64 {
	lastSoftState := node.lastSoftState.Load()
	if lastSoftState == nil {
		return 0
	}

	return lastSoftState.Lead
}

// GetNodeID returns the ID of this node.
func (node *Node) GetNodeID() uint64 {
	return node.config.NodeID
}

// Logger returns the node's logger.
func (node *Node) Logger() logging.Logger {
	return node.logger
}

// WaitLeaderReady blocks until the leader's FSM has caught up after a
// leadership transition. During steady-state the channel is already
// closed, so this returns immediately.
func (node *Node) WaitLeaderReady(ctx context.Context) error {
	ch := node.leaderReady.Load()
	if ch == nil {
		return nil
	}

	select {
	case <-*ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LastPersistedIndex returns the FSM's last persisted Raft index.
func (node *Node) LastPersistedIndex() uint64 {
	return node.fsm.LastPersistedIndex()
}

// WaitForApplied delegates to the FSM. Used by the GetFSMDigest gRPC
// handler when the caller pins a target index for cross-node comparison.
func (node *Node) WaitForApplied(ctx context.Context, target uint64) error {
	return node.fsm.WaitForApplied(ctx, target)
}

// GetClusterState returns the current state of the Raft cluster.
// The rawNode.Status() call is dispatched to the orchestrate goroutine
// because rawNode is not thread-safe. lastPersistedIndex is sampled in the
// SAME closure to eliminate the OUTER temporal race: without it, status was
// captured on the orchestrate goroutine and LastPersistedIndex was loaded
// much later on the caller goroutine, with arbitrary cross-goroutine work
// (further Ready cycles, commits) interleaving — leading to an exaggerated
// skew between the two reported cursors.
//
// The same-closure capture does NOT make the pair monotonically related.
// `status.Applied` is bumped by rawNode.Advance on the orchestrate goroutine
// (after `readyTerminated`); `LastPersistedIndex` is bumped by
// Machine.publishApplied on the committer goroutine (after pb.batch.Commit()
// returns). Neither depends on the other, and the orchestrate select
// services clusterCommandCh before draining readyTerminated, so this closure
// can run between `publishApplied(I)` and `Advance(I)` — observing
// lpi = I, Applied = I-1. Consumers must treat the two cursors as
// independent (see clusterpb.RaftStatus.last_persisted_index).
func (node *Node) GetClusterState(ctx context.Context) (*clusterpb.ClusterState, error) {
	var (
		status             raft.Status
		lastPersistedIndex uint64
	)

	err := node.execClusterCommand(ctx, func() error {
		status = node.rawNode.Status()
		lastPersistedIndex = node.fsm.LastPersistedIndex()

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Get leader
	leaderID := status.Lead

	stateStr := strings.TrimPrefix(status.RaftState.String(), "State")

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
				Match:            prog.Match,
				Next:             prog.Next,
				State:            stateStr,
				PendingSnapshot:  prog.PendingSnapshot,
				RecentActive:     prog.RecentActive,
				MsgAppFlowPaused: prog.MsgAppFlowPaused,
				IsPaused:         prog.IsPaused(),
				IsLearner:        prog.IsLearner,
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

	// Build complete Raft status.
	//
	// `Applied` is the Raft-layer cursor: bumped by rawNode.Advance on the
	// orchestrate goroutine after `readyTerminated` is consumed.
	// `LastPersistedIndex` is the durable FSM-side cursor: bumped by
	// Machine.publishApplied on the committer goroutine after
	// pb.batch.Commit() returns. The two advance independently on different
	// goroutines (orchestrate vs committer), so a single snapshot can
	// observe either cursor temporarily ahead of the other — do NOT assume
	// LastPersistedIndex <= Applied.
	//
	// Anything that reads from Pebble (stale-consistency GetAccount, test
	// oracles, cross-node identity comparisons) MUST gate on
	// LastPersistedIndex — that is the only cursor that guarantees a Pebble
	// read will see entries up to that index. Gating on Applied races the
	// apply pipeline and returns stale data.
	raftStatus := &clusterpb.RaftStatus{
		State:              stateStr,
		Term:               hardState.Term,
		Leader:             leaderID,
		Applied:            status.Applied,
		Commit:             hardState.Commit,
		LastIndex:          lastIndex,
		Vote:               hardState.Vote,
		Progress:           progress,
		LastPersistedIndex: lastPersistedIndex,
	}

	clusterState := &clusterpb.ClusterState{
		State:      stateStr,
		Leader:     uint32(leaderID),
		Nodes:      nodes,
		LocalNode:  uint32(node.config.NodeID),
		RaftStatus: raftStatus,
	}

	// Populate local sync progress
	clusterState.SyncProgress = &clusterpb.SyncProgress{
		Status: node.applier.StatusString(),
	}
	if sp := node.applier.GetSyncProgress(); sp != nil {
		clusterState.SyncProgress.BytesReceived = sp.BytesReceived()
		clusterState.SyncProgress.BytesTotal = sp.BytesTotal()
	}

	return clusterState, nil
}

// IsHealthy returns true if the rawNode is connected to the cluster (leader or follower).
func (node *Node) IsHealthy() bool {
	ss := node.lastSoftState.Load()
	if ss == nil {
		return false
	}
	// Node is healthy if it's a leader or follower
	return ss.RaftState == raft.StateLeader || ss.RaftState == raft.StateFollower
}

// IsStarted returns true once the rawNode loop has produced a soft state,
// regardless of the current Raft role (PreCandidate, Candidate, Follower,
// Leader). It is a weaker liveness signal than IsHealthy and is intended for
// the StatefulSet readiness gate: a node that has started its Raft loop is
// ready to participate in elections and accept peer traffic, even when quorum
// is not yet established. Decoupling the StatefulSet OrderedReady gate from
// quorum availability is what prevents the cold-start deadlock where pod-0
// blocks indefinitely waiting for a leader that cannot be elected until
// pod-1 and pod-2 are launched.
func (node *Node) IsStarted() bool {
	return node.lastSoftState.Load() != nil
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

	// If Run() has already exited (e.g. task error or context cancellation),
	// skip the leadership transfer and stopChannel handshake.
	select {
	case <-node.runDone:
		node.logger.Infof("Run already exited, nothing to stop")

		return nil
	default:
	}

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
	case <-node.runDone:
		// Run() exited while we were trying to send on stopChannel
		// (e.g. a task crashed between our check and the send).
		return nil
	}
}

// SetObserver sets the observer that receives events emitted by the node.
func (node *Node) SetObserver(obs *Observer) {
	node.observer = obs
}

// proposeConfChangeAndWait proposes a ConfChange via the orchestrate loop and
// waits for it to be committed. Returns (true, nil) on commit, (false, nil) if
// the proposal was likely dropped (timeout), or (false, err) on error.
// The proposeFn is dispatched via execClusterCommand (rawNode is not thread-safe).
func (node *Node) proposeConfChangeAndWait(ctx context.Context, nodeID uint64, proposeFn func() error, timeout time.Duration) (bool, error) {
	future := futures.New[struct{}]()

	node.pendingConfChanges.Store(nodeID, future)
	defer node.pendingConfChanges.Delete(nodeID)

	err := node.execClusterCommand(ctx, proposeFn)
	if err != nil {
		return false, err
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	_, err = future.Wait(timeoutCtx)
	if err != nil {
		// Timeout (deadline exceeded) means the proposal was likely dropped — not an error.
		if timeoutCtx.Err() != nil && ctx.Err() == nil {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// retryConfChange acquires confChangeMu and retries a ConfChange proposal until
// it commits or the context is cancelled. etcd/raft silently drops ConfChange
// proposals when another is pending; this method handles that transparently.
func (node *Node) retryConfChange(ctx context.Context, nodeID uint64, name string, proposeFn func() error) error {
	node.confChangeMu.Lock()
	defer node.confChangeMu.Unlock()

	retryInterval := max(node.config.TickInterval*time.Duration(node.config.HeartbeatTick)*3, 500*time.Millisecond)

	for {
		committed, err := node.proposeConfChangeAndWait(ctx, nodeID, proposeFn, retryInterval)
		if err != nil {
			return err
		}

		if committed {
			return nil
		}

		node.logger.WithFields(map[string]any{
			"nodeID": nodeID,
		}).Infof("%s: retrying (previous proposal likely dropped due to pending ConfChange)", name)
	}
}

// AddLearner proposes adding a non-voting learner node to the Raft cluster.
// The call blocks until the ConfChange is committed through Raft consensus.
// Must be called on the leader.
func (node *Node) AddLearner(ctx context.Context, nodeID uint64, raftAddr, serviceAddr string) error {
	ccCtx, err := MarshalConfChangeContext(ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	})
	if err != nil {
		return fmt.Errorf("marshaling conf change context: %w", err)
	}

	return node.retryConfChange(ctx, nodeID, "AddLearner", func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}

		if _, ok := status.Progress[nodeID]; ok {
			return ErrNodeAlreadyInCluster
		}

		return node.rawNode.ProposeConfChange(raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeAddLearnerNode,
				NodeID: nodeID,
			}},
			Context: ccCtx,
		})
	})
}

// PromoteLearner proposes promoting a learner node to a full voter.
// The call blocks until the ConfChange is committed through Raft consensus.
// Must be called on the leader.
func (node *Node) PromoteLearner(ctx context.Context, nodeID uint64) error {
	return node.retryConfChange(ctx, nodeID, "PromoteLearner", func() error {
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

		return node.rawNode.ProposeConfChange(raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeAddNode,
				NodeID: nodeID,
			}},
		})
	})
}

// RemoveNode proposes removing a node (voter or learner) from the Raft cluster.
// The call blocks until the ConfChange is committed through Raft consensus.
// Must be called on the leader. Cannot remove the leader itself.
func (node *Node) RemoveNode(ctx context.Context, nodeID uint64) error {
	return node.retryConfChange(ctx, nodeID, "RemoveNode", func() error {
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

		return node.rawNode.ProposeConfChange(raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: nodeID,
			}},
		})
	})
}

// ForceRemoveNode removes a node from the Raft cluster by directly applying a
// ConfChange without going through Raft consensus. This bypasses the log
// replication path entirely, so it works even when quorum is lost (e.g. the
// node being removed is down and the cluster can't reach majority).
//
// WARNING: This is unsafe for normal operations. Only use it for permanently
// unreachable nodes where consensus-based removal would block indefinitely.
// The caller must ensure the removed node will never rejoin with stale state.
//
// Must be called on the leader.
// todo: add a blacklist to prevent staled node reconnection
func (node *Node) ForceRemoveNode(ctx context.Context, nodeID uint64) error {
	node.confChangeMu.Lock()
	defer node.confChangeMu.Unlock()

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

		// Apply the ConfChange directly (bypasses consensus).
		cc := raftpb.ConfChangeV2{
			Changes: []raftpb.ConfChangeSingle{{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: nodeID,
			}},
		}
		cs := node.rawNode.ApplyConfChange(cc)
		node.confState.Store(cs)

		node.RemovePeerAddress(nodeID)

		// Persist the updated ConfState in the WAL snapshot so that restarts
		// see the correct voter set.
		err := node.wal.UpdateSnapshotConfState(cs)
		if err != nil {
			return fmt.Errorf("persisting confstate after force-remove: %w", err)
		}

		node.logger.WithFields(map[string]any{
			"removedNodeID": nodeID,
			"voters":        cs.Voters,
			"learners":      cs.Learners,
		}).Infof("Force-removed node (bypassed consensus)")

		// Notify observers so bootstrap can clean up transport/service pool.
		node.observer.Emit(ConfChangeEvent{
			NodeID:     nodeID,
			ChangeType: raftpb.ConfChangeRemoveNode,
		})

		return nil
	})
}

// checkAndPromoteLearners checks all learner nodes and promotes those that are
// caught up (within AutoPromoteThreshold of the commit index).
// Must be called from the orchestrate loop (rawNode is not thread-safe).
func (node *Node) checkAndPromoteLearners() {
	// Skip if an external ConfChange operation (AddLearner, RemoveNode, etc.)
	// is in-flight. etcd/raft would silently drop our proposal anyway.
	if !node.confChangeMu.TryLock() {
		return
	}
	node.confChangeMu.Unlock()

	status := node.rawNode.Status()
	if status.RaftState != raft.StateLeader {
		return
	}

	now := time.Now()

	// Rate-limit: don't re-propose promotion for the same learner within this interval.
	const autoPromoteRetryInterval = 2 * time.Second

	for id, prog := range status.Progress {
		if !prog.IsLearner {
			// Node was promoted (or removed), clean up tracking.
			delete(node.lastAutoPromote, id)

			continue
		}

		if !prog.RecentActive || prog.Match == 0 {
			continue
		}

		if prog.Match+node.config.AutoPromoteThreshold >= status.Commit {
			if lastAttempt, ok := node.lastAutoPromote[id]; ok {
				if now.Sub(lastAttempt) < autoPromoteRetryInterval {
					continue
				}
			}

			node.lastAutoPromote[id] = now
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

			err := node.rawNode.ProposeConfChange(cc)
			if err != nil {
				node.logger.WithFields(map[string]any{
					"node_id": id,
					"error":   err,
				}).Errorf("Failed to propose learner promotion")
			}

			// Only propose one promotion per tick to avoid multiple concurrent proposals.
			return
		}
	}
}

// confStateContainsNode returns true if nodeID appears in the ConfState's
// Voters or Learners list.
func confStateContainsNode(cs raftpb.ConfState, nodeID uint64) bool {
	if slices.Contains(cs.Voters, nodeID) {
		return true
	}

	return slices.Contains(cs.Learners, nodeID)
}

// ExtractPeerAddressesFromEntries scans committed entries for ConfChange entries
// and extracts peer addresses from their Context field. For AddNode/AddLearnerNode
// the address is stored (latest wins); for RemoveNode the entry is deleted.
func ExtractPeerAddressesFromEntries(entries []raftpb.Entry) map[uint64]ConfChangeContext {
	peers := make(map[uint64]ConfChangeContext)

	for _, entry := range entries {
		cc, ok, err := unmarshalConfChangeV2(entry)
		if err != nil || !ok {
			continue
		}

		for _, change := range cc.Changes {
			switch change.Type {
			case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
				if len(cc.Context) == 0 {
					continue
				}

				ccCtx, err := UnmarshalConfChangeContext(cc.Context)
				if err != nil {
					continue
				}

				peers[change.NodeID] = ccCtx
			case raftpb.ConfChangeRemoveNode:
				delete(peers, change.NodeID)
			}
		}
	}

	return peers
}

// RecoveredPeers returns the peer addresses recovered from WAL entries and/or
// snapshots during node initialization. Used by bootstrap to restore transport
// connections without requiring a PeerStore file.
func (node *Node) RecoveredPeers() map[uint64]ConfChangeContext {
	return node.recoveredPeers
}

// SetPeerAddress upserts a peer's raft and service addresses.
// Called on ConfChange commits and by bootstrap for self-registration.
func (node *Node) SetPeerAddress(nodeID uint64, raftAddr, serviceAddr string) {
	node.peerAddressesMu.Lock()
	node.peerAddresses[nodeID] = ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	}
	node.peerAddressesMu.Unlock()
}

// RemovePeerAddress removes a peer's addresses.
// Called when a ConfChange removing a peer is committed.
func (node *Node) RemovePeerAddress(nodeID uint64) {
	node.peerAddressesMu.Lock()
	delete(node.peerAddresses, nodeID)
	node.peerAddressesMu.Unlock()
}

// PeerAddresses returns a copy of the current peer address map.
func (node *Node) PeerAddresses() map[uint64]ConfChangeContext {
	node.peerAddressesMu.RLock()
	defer node.peerAddressesMu.RUnlock()

	cp := make(map[uint64]ConfChangeContext, len(node.peerAddresses))
	maps.Copy(cp, node.peerAddresses)

	return cp
}

// wrapSnapshot serializes cluster-level metadata (peer addresses) into a
// NodeSnapshot for WAL storage.
func (node *Node) wrapSnapshot() ([]byte, error) {
	node.peerAddressesMu.RLock()
	peerAddrs := make([]*raftcmdpb.PeerAddress, 0, len(node.peerAddresses))
	for nodeID, addr := range node.peerAddresses {
		peerAddrs = append(peerAddrs, &raftcmdpb.PeerAddress{
			NodeId:         nodeID,
			RaftAddress:    addr.RaftAddress,
			ServiceAddress: addr.ServiceAddress,
		})
	}
	node.peerAddressesMu.RUnlock()

	ns := &raftcmdpb.NodeSnapshot{
		PeerAddresses: peerAddrs,
	}

	return ns.MarshalVT()
}
