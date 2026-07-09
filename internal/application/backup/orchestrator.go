// Package backup is the application-layer orchestrator for FSM-managed
// backup jobs.
//
// The infrastructure-layer package internal/infra/backup still owns the
// actual upload work (checkpoint + diff + S3 / filesystem driver). This
// layer wraps it with the Raft side: proposing BackupOrder.Start to
// open a job, running the inner driver, then proposing Complete / Fail
// at the end.
//
// Mutual exclusion lives in the FSM (see internal/infra/state/
// backup_jobs.go) — when two callers race a Start against the same
// destination, the second propose comes back with ErrBackupInProgress
// and the RPC handler returns to the client. No in-process mutex,
// no lease primitive, no clock-skew window.
//
// Liveness — "is the executor goroutine still alive?" — is observed
// in-memory through the ExecutorRegistry below. The cleanup loop reads
// from this registry rather than inferring liveness from FSM-side
// progress staleness. Same pattern the Archiver and MetadataConverter
// use: durable state for the work item, in-memory presence for "is
// someone working on it".
package backup

import (
	"context"
	"fmt"
	"sync"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TerminalProposeTimeout caps the time the orchestrator spends pushing
// a terminal Complete or Fail through Raft once the side effect
// (uploaded manifest, or upload failure) has already happened. A bare
// context.Background() would leak the handler goroutine forever if
// leadership is lost mid-propose; a client-bound context would skip
// the propose entirely on RPC cancellation, leaving the FSM slot
// stuck RUNNING until cleanup eventually fails it. The bounded
// background context covers both gaps with a single budget.
const TerminalProposeTimeout = 30 * time.Second

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source=orchestrator.go -destination=orchestrator_generated_test.go -package=backup

// Proposer is the high-level proposal API the orchestrator depends on.
// Implementations route the proposal through proposeTechnical, which
// (a) serialises the IndexTracker increment with the Raft propose under
// the tracker mutex — same discipline as every other technical proposal,
// closing the window where a backup proposal could hand an admission
// proposal a stale PredictedIndex — and (b) waits via WaitContext so
// leadership loss after Raft accept does not pin the caller goroutine.
//
// The concrete adapter lives in internal/bootstrap (backupProposer);
// the application layer is decoupled from plan to avoid the import
// cycle (plan imports state for ApplyResult).
type Proposer interface {
	Propose(ctx context.Context, cmd *raftcmdpb.Proposal) error
}

// ExecutorRegistry tracks which backup jobs have a live driver
// goroutine inside this leader process. Mirrors the "in-flight" map
// other workers (Archiver, MetadataConverter) maintain implicitly via
// channel reception; we make it explicit here because backups are
// RPC-driven, not channel-driven, and the cleanup loop needs to ask
// "is anyone still working on this job_id?" from a different
// goroutine.
//
// The registry is leader-local: on leadership transition the new
// leader sees an empty map, so every RUNNING entry inherited from the
// previous leader's BackupJobsState looks orphaned and gets failed by
// the next cleanup tick. That is exactly the behaviour we want — the
// previous leader's executor goroutine cannot survive a leadership
// change, so any RUNNING entry without a live executor IS orphaned.
type ExecutorRegistry struct {
	mu   sync.RWMutex
	jobs map[uint64]struct{}
}

// NewExecutorRegistry returns an empty registry.
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{jobs: map[uint64]struct{}{}}
}

// tryRegister adds jobID to the live set and returns true if the
// caller is the one that put it there. A subsequent register call for
// the same jobID returns false — the caller must NOT deregister at exit
// (the previous registrar still owns the marker). This protects against
// the crypto/rand jobID collision: when two orchestrator goroutines
// happen to generate the same jobID, the FSM rejects all but one of
// them; the loser must not pull the winner's liveness entry out from
// under it. Cases:
//   - winner: tryRegister=true → defers deregister, FSM accepts, runs.
//   - loser, same destination: tryRegister=false → FSM returns
//     ErrBackupInProgress (destination busy check runs before the
//     job_id check); skipping the deregister keeps the winner alive.
//   - loser, different destination: tryRegister=false → FSM returns
//     ErrBackupJobIDCollision; same outcome via the same skip.
func (r *ExecutorRegistry) tryRegister(jobID uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.jobs[jobID]; exists {
		return false
	}

	r.jobs[jobID] = struct{}{}

	return true
}

func (r *ExecutorRegistry) deregister(jobID uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, jobID)
}

// IsAlive reports whether the given job has a driver goroutine in this
// leader process. Read-locked; cheap to call from the cleanup tick.
func (r *ExecutorRegistry) IsAlive(jobID uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.jobs[jobID]

	return ok
}

// Orchestrator drives the FSM lifecycle of a backup job. The caller
// (today: the gRPC handlers on the leader) holds the connection open
// while the job runs; the orchestrator emits proposals on its behalf.
//
// Backups run under a leadership-scoped context. When leadership
// transfers, OnLeadershipChange(false) cancels that context, which
// propagates to every in-flight RunFull/RunIncremental, aborts the
// S3 SDK request mid-flight, and lets the goroutine exit promptly.
// Without this, an old leader's executor would keep writing to S3
// while the new leader's cleanup loop frees the destination slot —
// the second writer race flemzord raised on PR #401.
type Orchestrator struct {
	proposer Proposer
	store    *dal.Store
	logger   logging.Logger
	nodeID   uint64
	registry *ExecutorRegistry

	// maxSegmentBytes caps each incremental export segment before it splits.
	// 0 lets RunIncrementalBackup pick its default.
	maxSegmentBytes int64

	// mu guards leaderCtx / leaderCancel. RunFull/RunIncremental read
	// leaderCtx once and derive a child context for the run; the read
	// is under the lock so a concurrent OnLeadershipChange cannot
	// swap leaderCtx in the middle of the read.
	mu           sync.Mutex
	leaderCtx    context.Context
	leaderCancel context.CancelFunc
}

// NewOrchestrator wires the dependencies. nodeID is stamped onto each
// Start proposal so operator-facing history shows which node drove the
// job.
//
// The orchestrator starts in a non-leader state: leaderCtx is an
// already-cancelled context, so any RunFull/RunIncremental called
// before OnLeadershipChange(true) returns immediately with the
// cancellation error. Bootstrap calls OnLeadershipChange on startup
// (and on every transition thereafter) so the orchestrator's state
// tracks the node's actual leadership.
func NewOrchestrator(proposer Proposer, store *dal.Store, logger logging.Logger, nodeID uint64, registry *ExecutorRegistry, maxSegmentBytes int64) *Orchestrator {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	return &Orchestrator{
		proposer:        proposer,
		store:           store,
		logger:          logger.WithField("cmp", "backup-orchestrator"),
		nodeID:          nodeID,
		registry:        registry,
		maxSegmentBytes: maxSegmentBytes,
		leaderCtx:       cancelled,
		leaderCancel:    cancel,
	}
}

// Registry exposes the in-memory executor registry. The cleanup loop
// uses it to tell orphan jobs from live ones.
func (o *Orchestrator) Registry() *ExecutorRegistry { return o.registry }

// OnLeadershipChange is wired by bootstrap onto every node leadership
// transition. On gain: install a fresh leaderCtx so subsequent
// RunFull / RunIncremental calls run under a context that survives
// only while this node is leader. On loss: cancel that context — every
// in-flight upload's runCtx unblocks, the S3 SDK aborts its request,
// the executor goroutine returns runErr and deregisters itself. The
// proposeTerminal call on the way out also fails (we are no longer
// leader to propose) and is logged but not retried; the new leader's
// cleanup loop then proposes Fail for the orphan it inherited via FSM
// state.
func (o *Orchestrator) OnLeadershipChange(isLeader bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.leaderCancel()

	if isLeader {
		o.leaderCtx, o.leaderCancel = context.WithCancel(context.Background())
	}
}

// runContext returns a context that is cancelled when either the
// caller's ctx ends OR leadership is lost. The returned cancel must
// be called by the caller (deferred) to release the watcher goroutine
// even on the happy path.
func (o *Orchestrator) runContext(ctx context.Context) (context.Context, context.CancelFunc) {
	o.mu.Lock()
	leaderCtx := o.leaderCtx
	o.mu.Unlock()

	runCtx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-leaderCtx.Done():
			cancel()
		case <-runCtx.Done():
		}
	}()

	return runCtx, cancel
}

// RunFull proposes a full BackupOrder.Start, drives backup.RunBackup
// to upload checkpoint files, and proposes Complete (or Fail). It is
// the FSM-managed equivalent of what gRPC.ClusterService.Backup used
// to do inline behind an in-memory mutex.
//
// dst carries the destination tuple; the caller is responsible for
// translating an RPC request into it. storage is the already-configured
// driver (s3, filesystem...) that the inner runner uploads against.
func (o *Orchestrator) RunFull(ctx context.Context, dst *raftcmdpb.BackupDestination, storage backup.Storage) (*backup.Result, error) {
	jobID := commands.GenerateRandomID()

	// Wrap the caller's ctx so leadership loss cancels the upload
	// mid-flight — the S3 SDK respects ctx and aborts in-flight
	// requests, so the second-writer race after a leadership transfer
	// closes deterministically.
	runCtx, cancelRun := o.runContext(ctx)
	defer cancelRun()

	// Register BEFORE proposing Start so the cleanup tick — which can
	// fire between Start-apply and the registration — never sees a
	// RUNNING FSM entry without a live executor.
	//
	// tryRegister returns false when the jobID is already in the map.
	// That can only happen on a crypto/rand collision between two
	// concurrent orchestrator goroutines: one of them registered first
	// and owns the liveness marker; the FSM will reject our Start with
	// ErrBackupInProgress (same destination) or ErrBackupJobIDCollision
	// (different destination). In both cases we MUST NOT deregister on
	// exit — that would yank the other job's liveness and the cleanup
	// loop would orphan-fail a valid in-flight backup.
	deregisterOnExit := o.registry.tryRegister(jobID)
	defer func() {
		if deregisterOnExit {
			o.registry.deregister(jobID)
		}
	}()

	// ErrBackupInProgress / ErrBackupJobIDCollision are FSM business
	// rejections. They flow back as the typed sentinels through
	// proposeTechnical's apply-error wrapping (%w), so errors.Is at
	// the caller still recognises them.
	if err := o.proposeAndWait(runCtx, fullStart(jobID, dst, o.nodeID)); err != nil {
		return nil, fmt.Errorf("propose backup start: %w", err)
	}

	// Per-job checkpoint name so two parallel backups to different
	// destinations do not collide on the local filesystem. The FSM
	// mutex protects the destination slot, but the temporary
	// checkpoint directory is node-local and would otherwise be
	// shared.
	checkpointName := fmt.Sprintf("backup-%016x", jobID)

	result, runErr := backup.RunBackup(runCtx, o.logger, o.store, storage, dst.GetBucketId(), checkpointName)
	if runErr != nil {
		o.proposeTerminal(fullFail(jobID, runErr.Error()), jobID, "fail")

		return nil, runErr
	}

	termCtx, cancel := context.WithTimeout(context.Background(), TerminalProposeTimeout)
	defer cancel()

	if err := o.proposeAndWait(termCtx, fullComplete(jobID, result)); err != nil {
		o.logger.WithFields(map[string]any{
			"jobID": jobID,
			"error": err,
		}).Errorf("propose backup complete failed (job stays RUNNING until cleanup)")

		return nil, fmt.Errorf("propose backup complete: %w", err)
	}

	return result, nil
}

// RunIncremental mirrors RunFull for the incremental pipeline.
func (o *Orchestrator) RunIncremental(ctx context.Context, dst *raftcmdpb.BackupDestination, storage backup.Storage) (*backup.IncrementalBackupResult, error) {
	jobID := commands.GenerateRandomID()

	runCtx, cancelRun := o.runContext(ctx)
	defer cancelRun()

	// See RunFull for the tryRegister rationale — same reasoning.
	deregisterOnExit := o.registry.tryRegister(jobID)
	defer func() {
		if deregisterOnExit {
			o.registry.deregister(jobID)
		}
	}()

	if err := o.proposeAndWait(runCtx, incrementalStart(jobID, dst, o.nodeID)); err != nil {
		return nil, fmt.Errorf("propose incremental start: %w", err)
	}

	result, runErr := backup.RunIncrementalBackup(runCtx, o.logger, o.store, storage, dst.GetBucketId(), o.maxSegmentBytes)
	if runErr != nil {
		o.proposeTerminal(incrementalFail(jobID, runErr.Error()), jobID, "incremental fail")

		return nil, runErr
	}

	termCtx, cancel := context.WithTimeout(context.Background(), TerminalProposeTimeout)
	defer cancel()

	if err := o.proposeAndWait(termCtx, incrementalComplete(jobID, result)); err != nil {
		o.logger.WithFields(map[string]any{
			"jobID": jobID,
			"error": err,
		}).Errorf("propose incremental complete failed (job stays RUNNING until cleanup)")

		return nil, fmt.Errorf("propose incremental complete: %w", err)
	}

	return result, nil
}

// proposeTerminal pushes a terminal Fail proposal under a bounded
// background context. Used when the upload itself has already failed:
// by the time we get here there is no upstream caller left to surface
// a propose-level error to, so we log and continue. The bounded
// context (vs bare context.Background) caps how long a stuck propose
// can keep the handler goroutine alive if leadership is lost.
func (o *Orchestrator) proposeTerminal(populate func(*raftcmdpb.Proposal), jobID uint64, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), TerminalProposeTimeout)
	defer cancel()

	if err := o.proposeAndWait(ctx, populate); err != nil {
		o.logger.WithFields(map[string]any{
			"jobID": jobID,
			"kind":  kind,
			"error": err,
		}).Errorf("failed to propose backup terminal order (job stays RUNNING until cleanup)")
	}
}

// proposeAndWait builds a fresh proposal carrying the order populated
// by `populate` and routes it through the high-level Proposer. The
// adapter handles marshalling, IndexTracker serialisation, Raft
// accept, and FSM apply — all under WaitContext so ctx cancellation
// reaches both waits.
//
// Error contract: the FSM apply path returns the typed sentinels
// (ErrBackupInProgress / ErrBackupJobNotFound / ErrBackupJobIDCollision)
// directly; machine.applyProposal converts them into ApplyResult.Error
// and proposeTechnical re-wraps with %w so `errors.Is(err, sentinel)`
// works at every layer.
func (o *Orchestrator) proposeAndWait(ctx context.Context, populate func(*raftcmdpb.Proposal)) error {
	cmd := commands.NewCommand()
	populate(cmd)

	return o.proposer.Propose(ctx, cmd)
}

// ---------------------------------------------------------------------------
// Proposal builders
// ---------------------------------------------------------------------------
//
// Every backup lifecycle order rides the TechnicalUpdate envelope —
// same as cluster_config, idempotency_eviction, etc. — with empty
// coverage_bits because the backup apply path does not read from the
// FSM cache (BackupJobsState owns its own in-memory map).

// wrapFullBackupOrder embeds an already-built BackupOrder in a single
// TechnicalUpdate on the proposal. The TU's coverage_bits stay empty:
// backup handlers never read from the FSM cache, so no key needs to be
// declared in the execution plan.
func wrapFullBackupOrder(order *raftcmdpb.BackupOrder) func(*raftcmdpb.Proposal) {
	return func(p *raftcmdpb.Proposal) {
		p.TechnicalUpdates = append(p.TechnicalUpdates, &raftcmdpb.TechnicalUpdate{
			Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{BackupOrder: order},
		})
	}
}

// wrapIncrementalBackupOrder is the incremental-pipeline counterpart.
func wrapIncrementalBackupOrder(order *raftcmdpb.IncrementalBackupOrder) func(*raftcmdpb.Proposal) {
	return func(p *raftcmdpb.Proposal) {
		p.TechnicalUpdates = append(p.TechnicalUpdates, &raftcmdpb.TechnicalUpdate{
			Kind: &raftcmdpb.TechnicalUpdate_IncrementalBackupOrder{IncrementalBackupOrder: order},
		})
	}
}

func fullStart(jobID uint64, dst *raftcmdpb.BackupDestination, executor uint64) func(*raftcmdpb.Proposal) {
	return wrapFullBackupOrder(&raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId:          jobID,
			Destination:    dst,
			ExecutorNodeId: executor,
		}},
	})
}

func fullComplete(jobID uint64, result *backup.Result) func(*raftcmdpb.Proposal) {
	return wrapFullBackupOrder(&raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Complete{Complete: &raftcmdpb.BackupOrderComplete{
			JobId:             jobID,
			LastLogSequence:   result.LastLogSequence,
			LastAuditSequence: result.LastAuditSequence,
			LastAppliedIndex:  result.LastAppliedIndex,
			FilesUploaded:     uint64(result.FilesUploaded),
		}},
	})
}

func fullFail(jobID uint64, message string) func(*raftcmdpb.Proposal) {
	return wrapFullBackupOrder(&raftcmdpb.BackupOrder{
		Op: &raftcmdpb.BackupOrder_Fail{Fail: &raftcmdpb.BackupOrderFail{
			JobId:   jobID,
			Message: message,
		}},
	})
}

func incrementalStart(jobID uint64, dst *raftcmdpb.BackupDestination, executor uint64) func(*raftcmdpb.Proposal) {
	return wrapIncrementalBackupOrder(&raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
			JobId:          jobID,
			Destination:    dst,
			ExecutorNodeId: executor,
		}},
	})
}

func incrementalComplete(jobID uint64, result *backup.IncrementalBackupResult) func(*raftcmdpb.Proposal) {
	return wrapIncrementalBackupOrder(&raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Complete{Complete: &raftcmdpb.BackupOrderComplete{
			JobId:             jobID,
			LastLogSequence:   result.LastLogSequence,
			LastAuditSequence: result.LastAuditSequence,
			SegmentsUploaded:  uint64(result.SegmentsUploaded),
		}},
	})
}

func incrementalFail(jobID uint64, message string) func(*raftcmdpb.Proposal) {
	return wrapIncrementalBackupOrder(&raftcmdpb.IncrementalBackupOrder{
		Op: &raftcmdpb.IncrementalBackupOrder_Fail{Fail: &raftcmdpb.BackupOrderFail{
			JobId:   jobID,
			Message: message,
		}},
	})
}
