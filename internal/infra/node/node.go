package node

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
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

	// ErrNodeStaleProgress (EN-1436) is returned by AddLearner when the
	// leader already holds a Progress entry for the joining nodeID with a
	// non-zero Match — the leader believes it has already replicated log
	// entries to this node. A JoinAsLearner call only reaches AddLearner
	// when the caller has no CLUSTER_JOINED marker (empty/reprovisioned
	// WAL), so a non-zero Match means the leader's known match index points
	// at state the caller cannot possibly have. Proceeding would trigger
	// etcd-raft's "tocommit out of range" panic on the next MsgApp. This is
	// distinct from ErrNodeAlreadyInCluster (Match == 0: a benign idempotent
	// join or a not-yet-replicated learner refresh) and fires regardless of
	// whether the stored instance_id matches the incoming one — covering
	// both the identical-identity and the fresh-identity (WAL-wiped) rejoin.
	ErrNodeStaleProgress = errors.New("node already has stale raft progress on the leader")

	// ErrLearnerNotEligible is returned when trying to transfer leadership to a learner.
	ErrLearnerNotEligible = errors.New("learner nodes are not eligible for leadership")

	// ErrCannotRemoveSelf is returned when trying to remove the leader node itself.
	ErrCannotRemoveSelf = errors.New("cannot remove the leader node; transfer leadership first")

	// ErrNodeNotInCluster is returned when trying to remove a node that is not a cluster member.
	ErrNodeNotInCluster = errors.New("node is not a member of the cluster")

	// ErrNodeRemoved is returned by AddLearner / JoinAsLearner when the
	// (nodeID, instance_id) tuple is present in the removed-member
	// registry (EN-1045).
	ErrNodeRemoved = errors.New("node was previously removed from this cluster")

	// ErrNodeSyncing is returned by ReadIndexAndWait when the node is still catching up
	// (restoring a snapshot or replaying spool). Callers should forward the read to the leader.
	ErrNodeSyncing = errors.New("node is syncing")
)

// StaleRaftProgressReason is the machine-readable ErrorInfo reason attached to
// the FailedPrecondition status a leader returns when a JoinAsLearner call hits
// ErrNodeStaleProgress (EN-1436). The joining node matches on it to tell the
// stale-progress rejection apart from the removed-member blacklist rejection,
// which is also FailedPrecondition but calls for a different remediation.
const StaleRaftProgressReason = "STALE_RAFT_PROGRESS"

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

// LocalResponses is the channel through which the Applier signals to the
// orchestrate goroutine that a batch of MsgStorageAppendResp /
// MsgStorageApplyResp messages should be Step()-ed back into rawNode
// (closing the loop on async-storage writes). Owned by an fx provider so
// both NewApplier (writer) and NewNode (reader) receive the same instance
// via constructor injection — no setters.
type LocalResponses chan []*raftpb.Message

// NewLocalResponses constructs the LocalResponses channel. Buffered at 64 to
// absorb a few Ready cycles' worth of responses without blocking processReady.
func NewLocalResponses() LocalResponses {
	return make(LocalResponses, 64)
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
	confChanges []*raftpb.ConfChangeV2
	// applyResponses are MsgStorageApplyResp messages collected from
	// LocalApplyThread messages in rd.Messages. They are handed to
	// applier.Submit in finishReady and Step()-ed back into rawNode by the
	// applier AFTER applyDecodedEntriesToFSM (or spool append) completes — bumping
	// raft.Applied in lockstep with FSM-applied. Used only when
	// AsyncStorageWrites is enabled.
	applyResponses []*raftpb.Message
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

	readies         chan raft.Ready
	readyTerminated chan readyResult
	// localResponseCh receives MsgStorageAppendResp / MsgStorageApplyResp
	// messages (and any msgsAfterAppend responses they carry) produced by the
	// async-storage path. The orchestrate goroutine drains it and Step()s each
	// message into rawNode (rawNode is not thread-safe; only orchestrate may
	// touch it). Used only when raft.Config.AsyncStorageWrites is true.
	localResponseCh LocalResponses
	tasks           *taskSet
	stopChannel     chan chan struct{}
	runDone         chan struct{} // closed when Run() exits
	// membership owns the Raft peer-address state (Pebble + in-memory
	// cache) and the OnSnapshotInstalled / WriteConfChange callbacks
	// wired into Applier and Machine. EN-1413.
	membership         *membership.Membership
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
	membership *membership.Membership,
	localResponses LocalResponses,
) (*Node, error) {
	cfg.SetDefaults()

	snapshot, err := wal.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	var initialConfState *raftpb.ConfState

	if len(snapshot.GetMetadata().GetConfState().GetVoters()) == 0 {
		logger.Infof("Fresh start: WAL has no ConfState voters, creating initial snapshot")

		// Check for RESTORED marker from a completed backup restore
		marker, err := ReadRestoredMarker(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("reading restored marker: %w", err)
		}

		if marker != nil {
			// Restore mode: bootstrap from restored data. The restored store
			// carries lastAppliedIndex 1 (PrepareForBackup pins it), so the WAL
			// snapshot below lands at 1 and the new log starts at 2: raft then
			// has to route any fresh peer through the snapshot → checkpoint-sync
			// path instead of "catching it up" by replaying the log onto an
			// empty store, which would miss the entire restored FSM genesis.
			// FSM counters (nextLedgerID, nextSequenceID, etc.) are recovered
			// from Pebble before creating the WAL snapshot.
			logger.WithFields(map[string]any{
				"lastAppliedIndex":     marker.LastAppliedIndex,
				"lastAppliedTimestamp": marker.LastAppliedTimestamp,
			}).Infof("Detected RESTORED marker, bootstrapping from restored data")

			if err := recovery.RecoverState(); err != nil {
				return nil, fmt.Errorf("recovering FSM state from store: %w", err)
			}

			// A snapshot at 0 is no snapshot: the new log would claim
			// completeness from index 1 and a fresh learner could be caught
			// up by plain log replay onto an empty store, missing the whole
			// restored genesis. PrepareForBackup pins the index to >= 1; a 0
			// here means a marker written by another tool or by hand.
			if marker.LastAppliedIndex == 0 {
				return nil, fmt.Errorf(
					"invariant: RESTORED marker carries lastAppliedIndex 0; the restored genesis must occupy index >= 1 so joiners are forced through checkpoint sync — re-run restore finalize / store bootstrap to regenerate the marker")
			}

			initialConfState = &raftpb.ConfState{
				Voters: []uint64{cfg.NodeID},
			}

			if err := registerInitialPeers(membership, cfg, true); err != nil {
				return nil, err
			}

			if err := wal.CreateSnapshot(marker.LastAppliedIndex, initialConfState, nil); err != nil {
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
				voters = initialJoinVoters(cfg.Peers, cfg.NodeID)
				learners = []uint64{cfg.NodeID}
				logger.WithFields(map[string]any{
					"voters":   voters,
					"learners": learners,
				}).Infof("Join mode: initializing as learner, peers are voters")
			default:
				return nil, errors.New("first start requires --bootstrap or --join")
			}

			initialConfState = &raftpb.ConfState{
				Voters:   voters,
				Learners: learners,
			}

			// EN-1413: persist self (Bootstrap only — Join's self lands
			// later via the AddLearner ConfChange) + every cfg.Peers
			// entry BEFORE marking the cluster joined. This closes the
			// crash window where a node would restart with voter IDs in
			// the WAL ConfState but no peer addresses in Pebble — the
			// EN-1404-class failure where the bootstrap voter has no
			// durable address.
			if err := registerInitialPeers(membership, cfg, cfg.Bootstrap); err != nil {
				return nil, err
			}

			if err := wal.CreateSnapshot(0, initialConfState, nil); err != nil {
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
			"snapshotIndex": snapshot.GetMetadata().GetIndex(),
			"snapshotTerm":  snapshot.GetMetadata().GetTerm(),
			"voters":        snapshot.GetMetadata().GetConfState().GetVoters(),
			"learners":      snapshot.GetMetadata().GetConfState().GetLearners(),
			"nodeID":        cfg.NodeID,
			"bootstrap":     cfg.Bootstrap,
			"peerCount":     len(cfg.Peers),
		}).Infof("Restart detected: WAL already has ConfState (not a fresh start)")

		// Peer addresses come from Pebble (loaded below into recoveredPeers).
		// The WAL snapshot payload no longer carries them, and we no longer
		// scan WAL entries to overlay newer peers. The snapshot bookkeeping
		// (FSM cache reset, snapshotIndex update) is still required on the
		// install path; that lives in Synchronizer.
		switch {
		case snapshot.GetMetadata().GetIndex() > 0:
			logger.WithFields(map[string]any{
				"index":        snapshot.GetMetadata().GetIndex(),
				"snapshotSize": len(snapshot.GetData()),
			}).Infof("Restoring Machine from snapshot")

			if err := synchronizer.InstallSnapshot(context.Background(), snapshot); err != nil {
				panic(err)
			}

			logger.Infof("Installed FSM snapshot (snapshotIndex set, cache reset)")
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
				cfg.NodeID, initialConfState.GetVoters(), initialConfState.GetLearners(),
			)
		}
	}

	// EN-1413: drop any Pebble peer row whose NodeID is no longer in the
	// durable ConfState. Covers two crash windows: an interrupted
	// ForceRemoveNode (ConfState updated, Unregister not yet committed)
	// and a backup that carried source-cluster peers across restore
	// without the matching ConfState entries.
	if err := membership.ReconcileAgainstConfState(initialConfState); err != nil {
		return nil, fmt.Errorf("reconciling peers against ConfState: %w", err)
	}

	// EN-1413: upsert self unconditionally so a changed AdvertiseAddr /
	// ServiceAdvertiseAddr (e.g. a pod restart with a new advertised
	// endpoint) overwrites the stale Pebble row. Without this, the
	// node's checkpoint streaming would teach peers to dial the old
	// address. PersistInitialPeers on the fresh-start branches already
	// writes self for Bootstrap/Restore, but the restart branch never
	// did — that's the gap the previous bootstrap hook used to cover
	// before transport wiring moved into Membership.
	if err := membership.Register(cfg.NodeID, cfg.AdvertiseAddr, cfg.ServiceAdvertiseAddr, cfg.InstanceID); err != nil {
		return nil, fmt.Errorf("refreshing self in peer store: %w", err)
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
		localResponseCh:  localResponses,
		tasks:            newTaskSet(),
		stopChannel:      make(chan chan struct{}),
		pendingReads:     &SyncMap[uint64, *readIndexRequest]{},
		membership:       membership,
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
		node.lastObservedTerm.Store(hs.GetTerm())
	}

	node.confState.Store(initialConfState)

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
		// EN-1413: WAL replay applied ConfChange entries directly to
		// Pebble via WriteConfChange (FSM hot path) but did NOT update
		// the in-memory Membership cache or transport — finishReady,
		// which owns those side effects on the normal Ready path, does
		// not run during replay. Without this catch-up the recovered
		// node would dial the pre-crash peer set (or miss a newly
		// added peer) until the next snapshot install or restart.
		// Skipped when the store is not up to date: SynchronizeWithLeader
		// will install a leader checkpoint and OnSnapshotInstalled will
		// rehydrate then.
		if err := membership.Rehydrate(); err != nil {
			return nil, fmt.Errorf("rehydrating membership after replay: %w", err)
		}

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

	if walLastIdx <= lastSnap.GetMetadata().GetIndex()+node.applier.CompactionMargin() {
		return nil
	}

	appliedIndex, err := query.ReadLastAppliedIndex(node.applier.Store())
	if err != nil {
		return fmt.Errorf("reading applied index: %w", err)
	}

	// Only snapshot at the applied index (not walLastIdx) — the FSM may not
	// have processed every WAL entry yet.
	if appliedIndex <= lastSnap.GetMetadata().GetIndex() {
		return nil
	}

	node.logger.WithFields(map[string]any{
		"lastSnapshotIndex":  lastSnap.GetMetadata().GetIndex(),
		"walLastIndex":       walLastIdx,
		"appliedIndex":       appliedIndex,
		"entriesAccumulated": walLastIdx - lastSnap.GetMetadata().GetIndex(),
	}).Infof("WAL has excess entries, compacting before Raft start")

	compactionStart := time.Now()

	if err := node.wal.CreateSnapshot(appliedIndex, node.confState.Load(), nil); err != nil {
		return fmt.Errorf("saving snapshot: %w", err)
	}

	node.logger.WithFields(map[string]any{
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
	if node.fsm.LastPersistedIndex() <= lastSnap.GetMetadata().GetIndex() {
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
	if err := node.wal.CreateSnapshot(capturedIndex, node.confState.Load(), nil); err != nil {
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
	// are gone from the Raft WAL — but that only means data loss when the
	// entries are also missing everywhere else. Two distinct paths reach here:
	//
	//  1. Genuine data loss (applier status != statusOutOfSync). The maintenance
	//     invariant (SyncWAL before Compact) was violated; Pebble's applied truly
	//     lags the WAL and no external replica has the missing entries. Refuse
	//     to start.
	//
	//  2. Crashed post-InstallSnapshot, pre-SynchronizeWithLeader (applier
	//     status == statusOutOfSync). The follower processed a MsgSnap from the
	//     leader: etcd-raft compacted the WAL to snapshotIndex synchronously
	//     (node.go processReady), but the async SynchronizeWithLeader that
	//     materialises the leader's checkpoint into Pebble had not completed
	//     when the process died. RecoverAndReplay detected the gap
	//     (LastAppliedIndex < SnapshotIndex) and flagged the applier
	//     out-of-sync. This state is self-healing: processReady will re-trigger
	//     SyncSnapshot on the next SoftState carrying a leader (node.go
	//     ready-loop). Cap Applied to walSnap.Metadata.Index so etcd-raft can
	//     boot — semantically consistent with State.SnapshotIndex, which
	//     synchronizer.InstallSnapshot already set to the same value.
	if applied+1 < walFirstIdx {
		if node.applier.Status() != statusOutOfSync {
			return fmt.Errorf(
				"durability gap exceeds WAL retention: Pebble applied=%d, WAL firstIndex=%d, "+
					"WAL snapshot=%d. The compaction margin was overrun before Pebble fsync'd. "+
					"Restore from a Pebble checkpoint or contact ops",
				applied, walFirstIdx, walSnap.GetMetadata().GetIndex(),
			)
		}

		node.logger.WithFields(map[string]any{
			"applied":          applied,
			"walFirstIndex":    walFirstIdx,
			"walSnapshotIndex": walSnap.GetMetadata().GetIndex(),
		}).Errorf(
			"Pebble applied lags WAL snapshot — previous process crashed between " +
				"InstallSnapshot and SynchronizeWithLeader completion; applier is " +
				"out-of-sync, will re-sync from leader",
		)

		applied = walSnap.GetMetadata().GetIndex()
	}

	if walSnap.GetMetadata().GetIndex() > applied {
		// Should not happen with SyncWAL in doMaintenance; log loudly so the
		// regression is visible. Raft can still recover via redelivery of
		// [Applied+1, Commit] in CommittedEntries.
		node.logger.WithFields(map[string]any{
			"storeApplied":   applied,
			"walSnapshotIdx": walSnap.GetMetadata().GetIndex(),
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

	if hardState.GetCommit() < applied {
		node.logger.WithFields(map[string]any{
			"storeApplied":     applied,
			"walDurableCommit": hardState.GetCommit(),
		}).Infof("Pebble applied ahead of WAL durable commit, capping Applied")

		applied = hardState.GetCommit()
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
		// AsyncStorageWrites: local storage messages ride in rd.Messages
		// (MsgStorageAppend for LocalAppendThread, MsgStorageApply for
		// LocalApplyThread) instead of the Ready's legacy fields. Their
		// embedded response messages must be Step()-ed back into rawNode
		// after the storage work completes; rawNode.Advance MUST NOT be
		// called.
		AsyncStorageWrites: true,
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
		"term":      status.HardState.GetTerm(),
		"commit":    status.HardState.GetCommit(),
		"vote":      status.HardState.GetVote(),
		"voters":    node.confState.Load().GetVoters(),
		"learners":  node.confState.Load().GetLearners(),
	}).Infof("Raft node created — initial state")

	node.tasks.add(newTask(node.orchestrate))
	node.tasks.add(newTask(node.applier.Run))
	node.tasks.add(newTask(node.processReadies))
	node.tasks.add(newTask(node.runBackgroundMaintenance))
	node.tasks.run(ctx)

	// Signal ready as soon as the raft loop is up.
	//
	// /readyz is documented as intentionally permissive (see
	// internal/adapter/http/handlers_health.go): "returns 200 once the local
	// Raft loop has started, regardless of whether a leader has been elected".
	// The signal here is what the fx OnStart hook wrapping node.Run is
	// blocked on (see bootstrap/module.go); fx runs OnStart hooks serially,
	// so nothing that comes after — most importantly the HTTP server hook
	// that opens port 9000 — starts until this closes.
	//
	// Blocking `ready` on FSM catch-up produced a design bug: a follower
	// booting with an out-of-sync applier (fresh Pebble, WAL compacted past
	// its snapshot — see the EN-1431 series) never catches up until
	// SynchronizeWithLeader completes, which for a 17 GiB checkpoint can
	// take multiple minutes. During that window the HTTP server never
	// started, /readyz refused connections, kubelet marked the pod
	// permanently not-ready, and the StatefulSet's OrderedReady rollout
	// stalled — even though raft, spool, and apply were all functioning
	// exactly as designed. The syncing follower IS ready in every sense
	// that matters at k8s scheduling level; write traffic is forwarded to
	// the leader and reads that need FSM state have their own gating.
	//
	// Downstream consumers that legitimately need the FSM caught up to a
	// specific index (index builder, event manager, cluster-config
	// reconciler) call fsm.WaitForApplied on demand rather than piggybacking
	// on node.Run's ready signal.
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
			// v3.7: rd.HardState is a nil-able embedded pointer; use the getter for a nil-safe read.
			term := rd.GetTerm()
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

	// Trigger SyncSnapshot when the applier is out-of-sync. Reads lastSoftState
	// after the SoftState block above so both cases are covered by one
	// site: this Ready carries a fresh SoftState (Lead learned or changed),
	// or this Ready has none but a previous Ready already set lastSoftState.
	//
	// Without this fallback, a follower that booted through node.Run's
	// durability-gap recovery branch (Pebble empty, WAL compacted past
	// snapshot) stays statusOutOfSync forever: the applier spools every
	// MsgApp without applying to Pebble, so LastPersistedIndex never
	// advances, node.Run's WaitForApplied never returns, the readiness
	// probe never succeeds, and the Raft WAL grows unbounded (no
	// maintenance snapshot until LastPersistedIndex moves) until the WAL
	// PVC runs out of space. EN-1431 follow-up.
	//
	// Guarded on IsEmptySnap so we don't collide with the snapshot-processing
	// block below, which fires its own syncSnapshot for the leader that just
	// sent this snapshot. TrySyncSnapshot is non-blocking so back-to-back
	// Readies can't stack duplicate syncLeader items (which would interrupt
	// an in-flight checkpoint fetch via taskExecutor.Interrupt()). The
	// applier's own status transition (statusOutOfSync -> statusGated)
	// makes subsequent Readies skip this block once the request is picked up.
	if raft.IsEmptySnap(rd.Snapshot) && node.applier.Status() == statusOutOfSync && !isStopping(stop) {
		if ss := node.lastSoftState.Load(); ss != nil && ss.Lead != 0 {
			node.applier.TrySyncSnapshot(ss.Lead)
		}
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
		rdTerm := rd.GetTerm()
		for {
			cur := node.lastObservedTerm.Load()
			if cur >= rdTerm {
				break
			}

			if node.lastObservedTerm.CompareAndSwap(cur, rdTerm) {
				break
			}
		}
	}

	result := readyResult{rd: rd}

	if !raft.IsEmptySnap(rd.Snapshot) {
		snapshotStart := time.Now()

		snapshotDetails := map[string]any{
			"nodeID":       node.config.NodeID,
			"index":        rd.Snapshot.GetMetadata().GetIndex(),
			"snapshotSize": len(rd.Snapshot.GetData()),
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

		if err := node.synchronizer.InstallSnapshot(ctx, rd.Snapshot); err != nil {
			return readyResult{}, fmt.Errorf("installing snapshot: %w", err)
		}

		// Defer ReportSnapshot to orchestrate goroutine (rawNode is not thread-safe).
		result.snapshotApplied = true

		node.logger.WithFields(map[string]any{
			"duration": time.Since(snapshotStart).String(),
			"index":    rd.Snapshot.GetMetadata().GetIndex(),
		}).Infof("Snapshot from leader applied, starting checkpoint sync")

		// The snapshot is already persisted in WAL at this point. If syncSnapshot
		// fails (network issue, leader unavailable, etc.), the node transitions to
		// statusOutOfSync and will retry automatically when a leader is detected
		// via SoftState or on restart (isStoreUpToDate check).
		// Skip sync if the node is shutting down — RestoreCheckpoint reopens the
		// Pebble DB, and background tasks (bloom restore) would create iterators
		// that outlive the DB.Close() in the fx shutdown hook.
		//
		// EN-1413: SyncSnapshot only enqueues the restore on the applier
		// and returns immediately; the actual RestoreCheckpoint runs in
		// the applier's Run goroutine. The OnSnapshotInstalled hook wired
		// in NewNode (node.reloadPeersFromStore) is what refreshes the
		// peer-address cache once Pebble has been swapped — do NOT call
		// peerStore.LoadAll here, it would read the pre-restore DB.
		if !isStopping(stop) {
			node.applier.SyncSnapshot(node.lastSoftState.Load().Lead, stop)
		}
	}

	appendResponses, applyResponses, outbound := splitReadyMessages(node.config.NodeID, rd.Messages)
	result.applyResponses = applyResponses

	node.transport.Send(outbound)

	if len(appendResponses) > 0 {
		select {
		case node.localResponseCh <- appendResponses:
		case <-stop:
		}
	}

	// Extract conf changes from committed entries. The actual rawNode.ApplyConfChange
	// calls are deferred to the orchestrate goroutine (rawNode is not thread-safe).
	for _, entry := range rd.CommittedEntries {
		cc, ok, err := membership.UnmarshalConfChangeV2(entry)
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
		node.rawNode.ReportSnapshot(rd.Snapshot.GetMetadata().GetIndex(), raft.SnapshotFinish)

		// Re-sync the in-memory ConfState shadow from the just-installed
		// snapshot. wal.ApplySnapshot already persisted the correct ConfState;
		// without this, the reconcile block below loads the stale shadow and
		// overwrites the WAL with it (EN-1278). Runs before the conf-change
		// loop so a combined snapshot+conf-change Ready still layers the delta
		// on top of the correct baseline.
		node.confState.Store(rd.Snapshot.GetMetadata().GetConfState())
	}

	// Apply conf changes (rawNode.ApplyConfChange must run in orchestrate goroutine).
	// Collect pending futures to resolve AFTER the WAL ConfState update, so
	// callers waiting on ConfChange commit (AddLearner, PromoteLearner, etc.)
	// don't resume before the WAL is consistent.
	var pendingFutures []*futures.Future[struct{}]

	for _, cc := range result.confChanges {
		node.logger.
			WithFields(map[string]any{"transition": cc.GetTransition().String()}).
			Infof("Applying configuration change")
		node.confState.Store(node.rawNode.ApplyConfChange(cc))

		// Mirror the committed ConfChange into the in-memory cache +
		// transport so the next Raft tick already sees the new address.
		// The durable Pebble row was written by WriteConfChange inside
		// the FSM batch — atomic with the surrounding business writes,
		// idempotent across spool/WAL replay. Updating the cache here
		// too means the transport doesn't have to wait for the applier
		// tick to learn the new address. (rawNode.ApplyConfChange above
		// makes Raft start replicating to the new peer immediately, so
		// the transport must be wired by then.)
		err := membership.WalkConfChangeContexts(cc, func(t raftpb.ConfChangeType, nodeID uint64, ctx *membership.ConfChangeContext) error {
			switch t {
			case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode:
				if ctx != nil {
					node.membership.Set(nodeID, ctx.RaftAddress, ctx.ServiceAddress, ctx.InstanceID)
				}
			case raftpb.ConfChangeRemoveNode:
				node.membership.Remove(nodeID)
			}

			return nil
		})
		if err != nil {
			return err
		}

		// Collect pending ConfChange futures (resolved below after WAL
		// update) and emit the antithesis lifecycle ping for the
		// fault-injector. Loops over every change.NodeID — including
		// PromoteLearner and UpdateNode types that membership.WalkConfChangeContexts
		// skips — because pending futures are keyed by NodeID regardless
		// of transition type.
		for _, change := range cc.GetChanges() {
			if f, ok := node.pendingConfChanges.LoadAndDelete(change.GetNodeId()); ok {
				pendingFutures = append(pendingFutures, f)
			}

			lifecycle.SendEvent("conf_change_committed", map[string]any{
				"nodeID":     node.config.NodeID,
				"targetNode": change.GetNodeId(),
				"changeType": change.GetType().String(),
			})
		}
	}

	// If the ConfState changed (e.g. a learner was added), update the WAL
	// snapshot's ConfState immediately. Without this, etcd/raft would send
	// the stale snapshot (which lacks the new node) before the applier's
	// async snapshot creation finishes, causing the new node to reject it.
	if cs := node.confState.Load(); cs != nil {
		snap, _ := node.wal.Snapshot()
		if !confStatesEqual(cs, snap.GetMetadata().GetConfState()) {
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
		lastCommitted := rd.CommittedEntries[len(rd.CommittedEntries)-1].GetIndex()
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

	// Submit committed entries to the Applier for async FSM application.
	// applyResponses (MsgStorageApplyResp) are deferred to the applier: they
	// are Step()-ed back into rawNode only after applyDecodedEntriesToFSM (or the
	// spool append) completes, so raft.Applied tracks FSM-applied.
	//
	// The guard is CommittedEntries-only by design. etcd/raft only emits
	// MsgStorageApply (source of applyResponses) when CommittedEntries > 0
	// (rawnode.go's needStorageApplyMsg). So `len(applyResponses) > 0 &&
	// len(CommittedEntries) == 0` is unreachable — a defensive OR would only
	// hide a future contract violation, since applyDecodedEntriesToFSM's decode+loop
	// would silently drop responses when entries is empty (CLAUDE.md #7).
	if len(rd.CommittedEntries) > 0 {
		node.applier.Submit(rd.CommittedEntries, node.confState.Load(), result.applyResponses, stop)
	}

	return nil
}

// splitReadyMessages classifies rd.Messages under AsyncStorageWrites into
// three flows:
//
//   - appendResponses: MsgStorageAppend.Responses whose To == nodeID (self-
//     directed acks — MsgStorageAppendResp + the leader's self-MsgAppResp).
//     They must be Step()-ed back into rawNode by the orchestrate goroutine
//     once the WAL append is durable (which it already is by the time this
//     runs).
//   - applyResponses: MsgStorageApply.Responses. Always self-directed
//     (newStorageApplyRespMsg sets To: r.id), so no split needed. They ride
//     via readyResult.applyResponses through applier.Submit and fire from
//     runCommitter after CommitPreparedBatch — aligning raft.Applied with
//     FSM-applied.
//   - outbound: everything else. This includes peer-directed messages that
//     were held back until durable inside MsgStorageAppend.Responses (a
//     follower's MsgAppResp, vote responses — see raft.go's msgsAfterAppend
//     path), as well as regular MsgApp / MsgHeartbeat / MsgVote already in
//     rd.Messages. All flow out through the transport.
//
// Pure function; called by processReady but split out so the self/peer
// split invariant (missing it broke cluster formation on the first attempt)
// stays testable without spinning up a full Node.
func splitReadyMessages(nodeID uint64, msgs []*raftpb.Message) (appendResponses, applyResponses, outbound []*raftpb.Message) {
	for _, m := range msgs {
		switch m.GetTo() {
		case raft.LocalAppendThread:
			for _, resp := range m.GetResponses() {
				if resp.GetTo() == nodeID {
					appendResponses = append(appendResponses, resp)
				} else {
					outbound = append(outbound, resp)
				}
			}
		case raft.LocalApplyThread:
			applyResponses = append(applyResponses, m.GetResponses()...)
		default:
			outbound = append(outbound, m)
		}
	}

	return appendResponses, applyResponses, outbound
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
	stepMessages := func(msgs []*raftpb.Message) error {
		for _, msg := range msgs {
			s := node.applier.Status()
			msgType := msg.GetType()
			if msgType == raftpb.MsgTimeoutNow && s != statusNormal {
				node.logger.Infof("Rejecting MsgTimeoutNow while syncing")

				continue
			}

			// Diagnostic: log messages stepped while a Ready is being processed,
			// as they can mutate rawNode state before Advance is called.
			if node.readyTerminated != nil && (msgType == raftpb.MsgApp || msgType == raftpb.MsgSnap) {
				node.logger.WithFields(map[string]any{
					"type":       msgType.String(),
					"from":       msg.GetFrom(),
					"term":       msg.GetTerm(),
					"logTerm":    msg.GetLogTerm(),
					"index":      msg.GetIndex(),
					"commit":     msg.GetCommit(),
					"entryCount": len(msg.GetEntries()),
				}).Infof("Stepping MsgApp/MsgSnap while Ready in flight")
			}

			err := node.rawNode.Step(msg)
			if err != nil {
				if errors.Is(err, raft.ErrStepPeerNotFound) {
					if node.logger.Enabled(logging.TraceLevel) {
						node.logger.Tracef("Ignoring message from unknown peer %d (type=%s)", msg.GetFrom(), msgType)
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

	// stepResponses drains a batch from node.localResponseCh: it Steps the
	// responses back into rawNode (bumping Applied for the batch that
	// runCommitter just committed) and then checks whether the new state
	// unlocks another Ready cycle. Broken out because the orchestrate select
	// arms drain localResponseCh from three different priority tiers, and
	// having one function makes it impossible for those three sites to drift
	// in error handling.
	stepResponses := func(msgs []*raftpb.Message) error {
		if err := stepMessages(msgs); err != nil {
			return err
		}

		maybeCreateReady()

		return nil
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
		case msgs := <-node.localResponseCh:
			if err := stepResponses(msgs); err != nil {
				return err
			}
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
			case msgs := <-node.localResponseCh:
				if err := stepResponses(msgs); err != nil {
					return err
				}
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
							"firstCommitted": result.rd.CommittedEntries[0].GetIndex(),
							"hasSnapshot":    !raft.IsEmptySnap(result.rd.Snapshot),
							"snapshotIndex":  result.rd.Snapshot.GetMetadata().GetIndex(),
						}).Tracef("Pre-Advance diagnostic")
					}

					// AsyncStorageWrites: Advance must not be called (panics).
					// Raft tracks ready acceptance via the response messages
					// Step()-ed back through localResponseCh.

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
				case msgs := <-node.localResponseCh:
					if err := stepResponses(msgs); err != nil {
						return err
					}
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

// GetClusterState returns the current state of the Raft cluster.
// The rawNode.Status() call is dispatched to the orchestrate goroutine
// because rawNode is not thread-safe. lastPersistedIndex is sampled in the
// SAME closure to eliminate the OUTER temporal race: without it, status was
// captured on the orchestrate goroutine and LastPersistedIndex was loaded
// much later on the caller goroutine, with arbitrary cross-goroutine work
// (further Ready cycles, commits) interleaving — leading to a larger skew
// between the two reported cursors.
//
// Under AsyncStorageWrites the two cursors are close but still advance on
// distinct goroutines: `status.Applied` is bumped when the orchestrate
// goroutine Step()s a MsgStorageApplyResp; `LastPersistedIndex` is bumped
// by Machine.publishApplied inside runCommitter's CommitPreparedBatch call,
// just before the response that carries that index is emitted on the sink.
// Ordering is publishApplied(I) → response send → orchestrate Step →
// rawNode.Applied = I. So this closure can run in the window between the
// FSM publish and the raft-side Step and observe lpi = I, Applied = I-1
// — but the reverse (Applied ahead of persisted) no longer happens.
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
	// `Applied` is the Raft-layer cursor: bumped when the orchestrate
	// goroutine Step()s the MsgStorageApplyResp for the batch (fired from
	// runCommitter after CommitPreparedBatch succeeds).
	// `LastPersistedIndex` is the durable FSM-side cursor: bumped by
	// Machine.publishApplied inside CommitPreparedBatch, immediately after
	// pb.batch.Commit() returns and before runCommitter emits the response.
	//
	// Under AsyncStorageWrites both are downstream of the same
	// CommitPreparedBatch call and advance in tight lockstep: `Applied`
	// arrives a channel-hop + orchestrate Step after `LastPersistedIndex`,
	// never before. So `LastPersistedIndex >= Applied` in this snapshot.
	//
	// Anything that reads from Pebble (stale-consistency GetAccount, test
	// oracles, cross-node identity comparisons) SHOULD still gate on
	// LastPersistedIndex — it is the durable-in-Pebble contract by name,
	// which is what those readers actually need. Gating on Applied works
	// too now, but couples the reader to a Raft-consensus cursor when the
	// semantic they want is "is Pebble caught up".
	raftStatus := &clusterpb.RaftStatus{
		State:              stateStr,
		Term:               hardState.GetTerm(),
		Leader:             leaderID,
		Applied:            status.Applied,
		Commit:             hardState.GetCommit(),
		LastIndex:          lastIndex,
		Vote:               hardState.GetVote(),
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
//
// postApplyFn (optional) is invoked while confChangeMu is still held, AFTER
// the ConfChange has been committed and the pending-future has been resolved
// by finishReady. Callers use it to block until a specific FSM side-effect
// is visible in Pebble — the future in finishReady resolves before the
// async applier has processed the entry, so subsequent operations acquiring
// confChangeMu may otherwise not observe the FSM write. Currently used by
// RemoveNode to guarantee the RemovedMemberEntry tombstone is visible
// before a racing JoinAsLearner's blacklist re-check runs (EN-1045).
func (node *Node) retryConfChange(ctx context.Context, nodeID uint64, name string, proposeFn func() error, postApplyFn func(ctx context.Context) error) error {
	node.confChangeMu.Lock()
	defer node.confChangeMu.Unlock()

	retryInterval := max(node.config.TickInterval*time.Duration(node.config.HeartbeatTick)*3, 500*time.Millisecond)

	for {
		committed, err := node.proposeConfChangeAndWait(ctx, nodeID, proposeFn, retryInterval)
		if err != nil {
			return err
		}

		if committed {
			if postApplyFn == nil {
				return nil
			}

			return postApplyFn(ctx)
		}

		node.logger.WithFields(map[string]any{
			"nodeID": nodeID,
		}).Infof("%s: retrying (previous proposal likely dropped due to pending ConfChange)", name)
	}
}

// existingLearnerAction is the decision AddLearner takes when the leader
// already tracks the joining nodeID in its raft Progress.
type existingLearnerAction int

const (
	// existingLearnerStaleProgress: the leader has already replicated log
	// entries to this node (Match > 0) AND the caller is a booting pod
	// (JoinAsLearner) presenting a real 16-byte instance_id. Such a caller
	// runs with a fresh, empty WAL and no CLUSTER_JOINED marker, so it cannot
	// own the replicated state the leader believes it has — proceeding would
	// trip etcd-raft's "tocommit out of range" panic. Fail fast.
	existingLearnerStaleProgress existingLearnerAction = iota
	// existingLearnerAlreadyInCluster: an idempotent join — nothing to do.
	// Covers Match == 0 with a matching (or absent) stored identity, AND the
	// admin AddLearner API path (no 16-byte incoming instance_id) for a node
	// that is already an active member (Match > 0). The admin path is an
	// operator action against an already-known cluster, never a fresh-WAL
	// boot, so it carries no "tocommit out of range" hazard and must keep the
	// pre-EN-1436 AlreadyExists semantics.
	existingLearnerAlreadyInCluster
	// existingLearnerNeedsRefresh: Match == 0 but the stored instance_id
	// differs from the joining pod's — refresh the peer row via
	// ConfChangeUpdateNode (admin AddLearner + boot, or a learner
	// reprovisioned before it received any entries).
	existingLearnerNeedsRefresh
)

// classifyExistingLearner decides what AddLearner must do when the leader's
// Progress already carries the joining nodeID (EN-1436).
//
// The stale-progress fail-fast is scoped strictly to the JoinAsLearner boot
// path, which is the only caller that presents a real 16-byte instance_id
// (see server_bootstrap.go). That path is a pod booting with a fresh, empty
// WAL: if the leader has already replicated to this nodeID (Match > 0), the
// next MsgApp would drive "tocommit out of range", so we reject and point the
// operator at remove-node --force. The Match > 0 check precedes the identity
// comparison so it fires on BOTH the identical-identity rejoin and the
// fresh-identity (WAL-wiped) rejoin — the latter would otherwise slip through
// as a benign ConfChangeUpdateNode refresh and re-introduce the crash loop.
//
// The admin AddLearner API path (server_cluster.go) passes no 16-byte
// instance_id. It is an idempotent operator call against an already-known
// cluster, not a fresh-WAL boot, so a Match > 0 there is a healthy already-
// active member: return AlreadyExists as before, NOT stale-progress — firing
// the fail-fast there would send the operator to the wrong remediation.
func classifyExistingLearner(match uint64, existingInstanceID []byte, hasRow bool, incomingInstanceID []byte) existingLearnerAction {
	isBootJoin := len(incomingInstanceID) == 16

	if match > 0 && isBootJoin {
		return existingLearnerStaleProgress
	}

	needsRefresh := hasRow && isBootJoin && !bytes.Equal(existingInstanceID, incomingInstanceID)
	if needsRefresh {
		return existingLearnerNeedsRefresh
	}

	return existingLearnerAlreadyInCluster
}

// AddLearner proposes adding a non-voting learner node to the Raft cluster.
// The call blocks until the ConfChange is committed through Raft consensus.
// instanceID (16 bytes, empty only from the admin cluster.AddLearner RPC
// where the target pod hasn't booted yet) travels in the marshaled
// ConfChangeContext so every node's FSM apply lands the same PeerAddress
// row (EN-1045).
//
// When the peer already exists in Raft (typical of the admin AddLearner +
// boot flow: admin pre-registered the row with a nil instance_id, then
// the pod boots and calls JoinAsLearner with its real instance_id), and
// we have a fresh 16-byte instance_id to fill in, this method proposes a
// ConfChangeUpdateNode instead of returning ErrNodeAlreadyInCluster —
// this refreshes the peer row across all nodes and unblocks
// checkAndPromoteLearners which otherwise defers promotion for rows
// without an instance_id.
//
// EN-1436: if the peer already exists with a non-zero Progress.Match (the
// leader has already replicated entries to it) AND this is a JoinAsLearner
// boot (a real 16-byte instanceID), this returns ErrNodeStaleProgress
// instead — such a caller boots with a fresh, empty WAL and no
// CLUSTER_JOINED marker, so both the AlreadyExists and the UpdateNode-refresh
// outcomes would let a "tocommit out of range" crash loop through. This check
// precedes the identity comparison so it fires on the fresh-identity
// (WAL-wiped) rejoin too. The admin AddLearner API path (nil instanceID) is
// exempt: a Match > 0 there is a healthy already-active member and keeps its
// idempotent ErrNodeAlreadyInCluster result.
//
// Must be called on the leader.
func (node *Node) AddLearner(ctx context.Context, nodeID uint64, raftAddr, serviceAddr string, instanceID []byte) error {
	ccCtx, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
		InstanceID:     instanceID,
	})
	if err != nil {
		return fmt.Errorf("marshaling conf change context: %w", err)
	}

	return node.retryConfChange(ctx, nodeID, "AddLearner", func() error {
		status := node.rawNode.Status()
		if status.RaftState != raft.StateLeader {
			return ErrNotLeader
		}

		// EN-1045 defense against a race between JoinAsLearner admission
		// (Pebble IsRemoved check) and RemoveNode commit: re-check the
		// blacklist here, inside the confChangeMu-serialized path. Any
		// prior RemoveNode has fully applied by now (retryConfChange
		// waits for commit), so a tombstone written in the interval is
		// visible. Empty instance_id (admin AddLearner without a booted
		// pod) can't be blacklisted, so we skip the check.
		if len(instanceID) == 16 {
			removed, checkErr := node.membership.IsRemoved(nodeID, instanceID)
			if checkErr != nil {
				return fmt.Errorf("checking removed-member registry for %d: %w", nodeID, checkErr)
			}

			if removed {
				return ErrNodeRemoved
			}
		}

		if prog, ok := status.Progress[nodeID]; ok {
			existing, hasRow := node.membership.GetInstanceID(nodeID)

			switch classifyExistingLearner(prog.Match, existing, hasRow, instanceID) {
			case existingLearnerStaleProgress:
				return ErrNodeStaleProgress
			case existingLearnerAlreadyInCluster:
				return ErrNodeAlreadyInCluster
			case existingLearnerNeedsRefresh:
				return node.rawNode.ProposeConfChange(&raftpb.ConfChangeV2{
					Changes: []*raftpb.ConfChangeSingle{{
						Type:   new(raftpb.ConfChangeUpdateNode),
						NodeId: new(nodeID),
					}},
					Context: ccCtx,
				})
			}
		}

		return node.rawNode.ProposeConfChange(&raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{
				Type:   new(raftpb.ConfChangeAddLearnerNode),
				NodeId: new(nodeID),
			}},
			Context: ccCtx,
		})
	}, nil)
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

		return node.rawNode.ProposeConfChange(&raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{
				Type:   new(raftpb.ConfChangeAddNode),
				NodeId: new(nodeID),
			}},
		})
	}, nil)
}

// RemoveNode proposes removing a node (voter or learner) from the Raft cluster.
// The call blocks until the ConfChange is committed through Raft consensus.
// Must be called on the leader. Cannot remove the leader itself.
//
// EN-1045: the removed peer's instance_id (looked up from Membership) is
// packed into the ConfChange context so every node's FSM apply lands a
// matching RemovedMemberEntry atomically with the peer row delete — a
// still-alive pod at the same nodeID cannot silently rejoin and be
// auto-promoted after this ConfChange commits.
func (node *Node) RemoveNode(ctx context.Context, nodeID uint64) error {
	// The target's identity is captured INSIDE the propose closure below,
	// after confChangeMu has been acquired. Reading it outside would race
	// with a concurrent ConfChangeUpdateNode (reprovisioned learner
	// refresh) that mutates the Membership row: we'd blacklist the stale
	// identity and let the current pod rejoin freely.
	var (
		capturedInstanceID      []byte
		capturedBlacklistableID bool
	)

	proposeFn := func() error {
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

		// Look up the target's identity from the in-memory Membership
		// cache (updated by finishReady when the prior ConfChange
		// resolved its future). We're inside the confChangeMu-
		// serialized path, so no other ConfChangeUpdateNode can race
		// with this read — any pending refresh from a reprovisioned
		// learner has fully applied before this closure runs.
		instanceID, hasIdentity := node.membership.GetInstanceID(nodeID)
		capturedInstanceID = instanceID
		capturedBlacklistableID = hasIdentity && len(instanceID) == 16

		// Missing instance_id means the peer's row has no identity —
		// either the row was created via the admin cluster.AddLearner
		// path without the target ever booting (phantom learner), or
		// it is a bootstrap initial peer that has not yet joined.
		// Both cases are legal: propose the removal without a Context,
		// and WriteConfChange will delete the peer row without writing
		// a blacklist entry (nothing to blacklist).
		var ccCtx []byte

		if capturedBlacklistableID {
			marshaled, err := membership.MarshalConfChangeContext(membership.ConfChangeContext{
				InstanceID: instanceID,
			})
			if err != nil {
				return fmt.Errorf("marshaling remove-node context for %d: %w", nodeID, err)
			}

			ccCtx = marshaled
		}

		return node.rawNode.ProposeConfChange(&raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{
				Type:   new(raftpb.ConfChangeRemoveNode),
				NodeId: new(nodeID),
			}},
			Context: ccCtx,
		})
	}

	// EN-1045: after the ConfChange commits (raft-level), also wait for
	// the async applier to have persisted the RemovedMemberEntry
	// tombstone to Pebble before releasing confChangeMu. This closes a
	// TOCTOU race where a concurrent JoinAsLearner acquiring the mutex
	// next would otherwise re-check the blacklist and miss the tombstone
	// still queued behind the applier's async submit.
	postApplyFn := func(ctx context.Context) error {
		if !capturedBlacklistableID {
			return nil
		}

		return node.waitForBlacklistApplied(ctx, nodeID, capturedInstanceID)
	}

	return node.retryConfChange(ctx, nodeID, "RemoveNode", proposeFn, postApplyFn)
}

// waitForBlacklistApplied polls the removed-member registry on the local
// Pebble store until (nodeID, instanceID) is visible or the context is
// cancelled. Used by RemoveNode to bridge the gap between raft commit
// (future resolved in finishReady) and FSM apply (RemovedMemberEntry
// written by the async applier). Runs while confChangeMu is held.
func (node *Node) waitForBlacklistApplied(ctx context.Context, nodeID uint64, instanceID []byte) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.Now().Add(5 * time.Second)

	for {
		removed, err := node.membership.IsRemoved(nodeID, instanceID)
		if err != nil {
			return fmt.Errorf("polling removed-member visibility for %d: %w", nodeID, err)
		}

		if removed {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for RemovedMemberEntry(%d) to be applied", nodeID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
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
// EN-1045: when the removed peer's instance_id is known (present in the
// Membership row), the same lifecycle path also writes a RemovedMemberEntry
// atomically with the peer row delete — a still-alive pod at that (nodeID,
// instance_id) cannot silently rejoin even by racing back before the
// StatefulSet shrinks. Followers converge via the next snapshot.
//
// Must be called on the leader.
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

		// Look up the target peer's identity before applying the
		// ConfChange — Membership.GetInstanceID reads the in-memory
		// cache populated at boot from Pebble. Missing instance_id
		// means the peer has a row without identity (phantom learner
		// added via admin cluster.AddLearner without ever booting, or
		// a bootstrap initial peer that never joined). Force-removing
		// such a peer is legal; the blacklist write is skipped
		// because there is nothing to blacklist.
		instanceID, hasIdentity := node.membership.GetInstanceID(nodeID)
		hasIdentity = hasIdentity && len(instanceID) == 16

		// Apply the ConfChange directly (bypasses consensus).
		cc := &raftpb.ConfChangeV2{
			Changes: []*raftpb.ConfChangeSingle{{
				Type:   new(raftpb.ConfChangeRemoveNode),
				NodeId: new(nodeID),
			}},
		}
		cs := node.rawNode.ApplyConfChange(cc)
		node.confState.Store(cs)

		// Order matters here. ForceRemoveNode bypasses the Raft log, so
		// there is no EntryConfChange the FSM replay can re-apply on
		// restart to reconcile a mismatch between WAL ConfState and the
		// Pebble peer row. Persist the ConfState first so any crash
		// between the two durable writes lands on the safe side: the
		// restored cluster has the peer removed from its voter set,
		// with a possibly-stale (harmless) Pebble row that LoadAll picks
		// up but the raft state machine ignores. The opposite order
		// would leave a configured voter with no dialable address.
		// EN-1413.
		err := node.wal.UpdateSnapshotConfState(cs)
		if err != nil {
			return fmt.Errorf("persisting confstate after force-remove: %w", err)
		}

		// EN-1045: when we know the target's identity, land the
		// blacklist entry atomically with the peer row delete in a
		// single Pebble batch. For phantom learners without an
		// identity (see hasIdentity comment above), fall back to
		// the plain peer row delete — nothing to blacklist.
		if hasIdentity {
			if err := node.membership.UnregisterAndBlacklist(nodeID, instanceID, uint64(time.Now().UnixMicro())); err != nil {
				return fmt.Errorf("force-remove atomic batch: %w", err)
			}
		} else {
			if err := node.membership.Unregister(nodeID); err != nil {
				return fmt.Errorf("removing peer after force-remove: %w", err)
			}
		}

		node.logger.WithFields(map[string]any{
			"removedNodeID": nodeID,
			"voters":        cs.GetVoters(),
			"learners":      cs.GetLearners(),
			"blacklisted":   hasIdentity,
		}).Infof("Force-removed node (bypassed consensus)")

		// UnregisterAndBlacklist above already wired the peer out of
		// transport / service pool, so no observer event is needed.

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

		if prog.Match+node.config.AutoPromoteThreshold >= status.GetCommit() {
			if lastAttempt, ok := node.lastAutoPromote[id]; ok {
				if now.Sub(lastAttempt) < autoPromoteRetryInterval {
					continue
				}
			}

			// EN-1045: refuse to promote a learner whose (nodeID,
			// instance_id) has been blacklisted. Belt-and-suspenders
			// check — JoinAsLearner admission should have refused
			// this rejoin earlier. Missing instance_id here means
			// the learner has a row without identity (admin
			// AddLearner without a booted peer yet); skip promotion
			// this tick and try again once the peer refreshes its
			// row via JoinAsLearner.
			instanceID, hasIdentity := node.membership.GetInstanceID(id)
			if !hasIdentity || len(instanceID) != 16 {
				node.logger.WithFields(map[string]any{
					"node_id": id,
				}).Infof("Auto-promote: learner has no instance_id yet; deferring promotion")

				continue
			}

			removed, checkErr := node.membership.IsRemoved(id, instanceID)
			if checkErr != nil {
				node.logger.WithFields(map[string]any{
					"node_id": id,
					"error":   checkErr,
				}).Errorf("Auto-promote: reading removed-member registry failed; skipping promotion this tick")

				continue
			}

			if removed {
				node.logger.WithFields(map[string]any{
					"node_id":    id,
					"instanceID": hex.EncodeToString(instanceID),
				}).Infof("Auto-promote: refusing blacklisted learner (EN-1045); operator must forget-removed to re-admit")

				continue
			}

			node.lastAutoPromote[id] = now
			node.logger.WithFields(map[string]any{
				"node_id":   id,
				"match":     prog.Match,
				"commit":    status.HardState.GetCommit(),
				"threshold": node.config.AutoPromoteThreshold,
			}).Infof("Auto-promoting learner to voter")

			cc := &raftpb.ConfChangeV2{
				Changes: []*raftpb.ConfChangeSingle{
					{
						Type:   new(raftpb.ConfChangeAddNode),
						NodeId: new(id),
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
func confStateContainsNode(cs *raftpb.ConfState, nodeID uint64) bool {
	if cs == nil {
		return false
	}

	if slices.Contains(cs.GetVoters(), nodeID) {
		return true
	}

	return slices.Contains(cs.GetLearners(), nodeID)
}

// initialJoinVoters returns the voter list for the join-mode initial WAL
// snapshot. It maps every peer discovered from the existing cluster to its
// ID and excludes self.
//
// The exclusion matters when this node was previously a cluster member and
// is now rejoining with fresh WAL — e.g. after a scale-down/scale-up cycle
// that removed its Pod but left the leader's status.Progress carrying the
// node ID from an earlier auto-promote. discoverPeersFromCluster echoes
// whatever the leader's Progress contains, so self can appear in cfg.Peers.
// Passing that ID both here (as voter) AND to cfg.NodeID's Learners entry
// produces the raft-invalid ConfState `Voters=[..., self], Learners=[self]`:
// on the next boot, raft.newRaft's assertConfStatesEquivalent detects the
// mismatch between the snapshot's ConfState and the tracker-restored
// equivalent (which normalises self into voters only) and panics with
// "ConfStates not equivalent after sorting", producing a permanent
// CrashLoopBackOff that no amount of retries recovers from.
func initialJoinVoters(peers []Peer, selfID uint64) []uint64 {
	voters := make([]uint64, 0, len(peers))
	for _, p := range peers {
		if p.ID == selfID {
			continue
		}

		voters = append(voters, p.ID)
	}

	return voters
}

// registerInitialPeers writes cfg.Peers (and self when includeSelf is
// true) into Membership before the WAL snapshot / CLUSTER_JOINED marker
// lands, so a crash cannot leave a durable ConfState without the
// matching peer addresses in Pebble (EN-1413).
//
// includeSelf is true for Bootstrap and Restore; false for Join
// (self's address lands later via the AddLearner ConfChange applied
// through WriteConfChange).
func registerInitialPeers(m *membership.Membership, cfg NodeConfig, includeSelf bool) error {
	if includeSelf {
		if err := m.Register(cfg.NodeID, cfg.AdvertiseAddr, cfg.ServiceAdvertiseAddr, cfg.InstanceID); err != nil {
			return fmt.Errorf("persisting self in peer store: %w", err)
		}
	}

	// Initial peers' instance IDs are not known at cluster-formation time
	// — each peer generates its own on first boot and reports it via
	// JoinAsLearner. Persist with empty InstanceID; the row is refreshed
	// by WriteConfChange when the peer later goes through ConfChange
	// apply (EN-1045).
	for _, p := range cfg.Peers {
		if err := m.Register(p.ID, p.Address, p.ServiceAddress, nil); err != nil {
			return fmt.Errorf("persisting initial peer %d: %w", p.ID, err)
		}
	}

	return nil
}
