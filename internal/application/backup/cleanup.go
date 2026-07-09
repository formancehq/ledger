package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// DefaultCleanupInterval is the wall-clock cadence of the cleanup loop.
// Used to pace the scan, never as part of a staleness decision.
const DefaultCleanupInterval = 30 * time.Second

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source=cleanup.go -destination=cleanup_generated_test.go -package=backup

// StaleProvider is the slice of the FSM state used by the cleanup loop.
// It is satisfied by *state.BackupJobsState; the indirection keeps the
// cleanup loop testable without spinning up a Machine.
type StaleProvider interface {
	ForEachActive(func(*raftcmdpb.BackupJob))
}

// LeaderProbe gates cleanup on the leader-only operation guarantee.
// On followers the FSM applies every Fail proposal we'd push, so a
// follower running cleanup would multiply the network cost without
// changing the outcome.
type LeaderProbe interface {
	IsLeader() bool
}

// LiveJobsProbe answers "is there a driver goroutine inside this leader
// process working on this job_id?". Implemented by the Orchestrator's
// ExecutorRegistry. The cleanup loop reads from it to tell live RUNNING
// entries (Orchestrator is still uploading) from orphaned RUNNING
// entries (a previous leader's executor disappeared and left the FSM
// slot taken).
type LiveJobsProbe interface {
	IsAlive(jobID uint64) bool
}

// Cleanup is the leader-only background loop that fails out orphaned
// backup jobs.
//
// What "orphaned" means here: a RUNNING entry in the FSM's
// BackupJobsState whose driver goroutine is no longer in this leader's
// process. This happens in two paths:
//
//  1. Process restart: the new process re-hydrates BackupJobsState from
//     Pebble via CacheSnapshotter.RestoreFromStore, but the executor
//     goroutine from before the restart is gone.
//  2. Leadership transfer mid-backup: the previous leader's goroutine
//     dies with its leadership context; the new leader inherits the
//     RUNNING entries through Raft but did not spawn the goroutines.
//
// In both cases the RUNNING entry has no LiveJobsProbe.IsAlive match.
// The cleanup tick proposes Fail to free the destination so a fresh
// client retry can take over.
//
// Notably absent: any "staleness" math based on wall-clock or
// applied-index gap. Earlier iterations of this loop inferred
// orphan-ness from absence-of-progress, which forced the orchestrator
// to ship heartbeat proposals and tied the cleanup correctness to the
// tuning of two thresholds (StaleIndexGap, StaleWallClockGap). That
// signal was indirect and prone to false positives — a slow but
// healthy upload would be killed. The in-memory LiveJobsProbe is a
// direct signal and removes the need for any timing heuristic. Same
// pattern as Archiver / MetadataConverter / Mirror: durable state
// describes the work item, in-memory presence describes the worker.
type Cleanup struct {
	state    StaleProvider
	proposer Proposer
	probe    LeaderProbe
	live     LiveJobsProbe
	logger   logging.Logger

	Interval time.Duration
}

// NewCleanup wires the dependencies. Defaults for Interval are filled
// in if the caller leaves them at zero so the service is usable with
// NewCleanup(...).Run(ctx) in fx.
func NewCleanup(s StaleProvider, proposer Proposer, probe LeaderProbe, live LiveJobsProbe, logger logging.Logger) *Cleanup {
	return &Cleanup{
		state:    s,
		proposer: proposer,
		probe:    probe,
		live:     live,
		logger:   logger.WithField("cmp", "backup-cleanup"),
		Interval: DefaultCleanupInterval,
	}
}

// Run blocks until ctx is cancelled, ticking the cleanup once per
// Interval. Each tick is a no-op when this node is not the leader —
// the followers' job-state map is a follower of the FSM's, not an
// independent observer.
func (c *Cleanup) Run(ctx context.Context) {
	if c.Interval <= 0 {
		c.Interval = DefaultCleanupInterval
	}

	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Cleanup) tick(ctx context.Context) {
	if !c.probe.IsLeader() {
		return
	}

	// Collect the IDs of orphan jobs while holding the FSM's read lock
	// (via ForEachActive), then release before proposing — Propose can
	// block on Raft and we don't want to keep the lock during that.
	type orphan struct {
		jobID uint64
		kind  raftcmdpb.BackupKind
	}

	var orphans []orphan

	c.state.ForEachActive(func(job *raftcmdpb.BackupJob) {
		if job.GetStatus() != raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_RUNNING {
			return
		}

		jobID := job.GetJobId()
		if c.live.IsAlive(jobID) {
			return
		}

		c.logger.WithFields(map[string]any{
			"jobID":          jobID,
			"kind":           job.GetKind().String(),
			"executorNodeId": job.GetExecutorNodeId(),
		}).Infof("backup job has no live executor on this leader; proposing Fail to free destination slot")

		orphans = append(orphans, orphan{jobID: jobID, kind: job.GetKind()})
	})

	for _, o := range orphans {
		if err := c.failOrphan(ctx, o.jobID, o.kind); err != nil {
			c.logger.WithFields(map[string]any{
				"jobID": o.jobID,
				"error": err,
			}).Errorf("cleanup: failed to propose orphan-fail (will retry next tick)")
		}
	}
}

func (c *Cleanup) failOrphan(ctx context.Context, jobID uint64, kind raftcmdpb.BackupKind) error {
	cmd := commands.NewCommand()
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentBackup)
	failMessage := "orphan: no live executor on this leader"

	switch kind {
	case raftcmdpb.BackupKind_BACKUP_KIND_FULL:
		fullFail(jobID, failMessage)(cmd)
	case raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL:
		incrementalFail(jobID, failMessage)(cmd)
	default:
		return fmt.Errorf("unknown backup kind: %v", kind)
	}

	// Route through the high-level Proposer (the bootstrap adapter
	// calls proposeTechnical). Both the Raft accept and FSM apply waits
	// are context-aware inside that helper, so leadership loss after
	// Raft accept does not pin this goroutine — important for the
	// cleanup loop which must exit promptly on fx stop.
	// ErrBackupJobNotFound means a different executor already
	// terminated the job in the same tick; that's benign and not worth
	// surfacing.
	if err := c.proposer.Propose(ctx, cmd); err != nil && !errors.Is(err, state.ErrBackupJobNotFound) {
		return err
	}

	return nil
}
