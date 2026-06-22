package state

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// applyBackupOrder dispatches one full-backup lifecycle order against the
// FSM-managed BackupJobsState. Per-job rejections — destination already
// running, wrong job_id, job_id already used on another destination —
// surface as the typed sentinels ErrBackupInProgress,
// ErrBackupJobNotFound, ErrBackupJobIDCollision. machine.applyProposal
// recognises those errors and translates them into ApplyResult.Error so
// the proposer learns about the rejection without an FSM-level abort.
//
// Any other returned error (Pebble write failure, marshalling, unknown
// op type) is FSM-fatal — the apply can't proceed in a consistent state.
//
// The lifecycle is reduced to Start / Complete / Fail. Progress
// proposals were removed: liveness is observed in-memory on the leader
// (see internal/application/backup/cleanup.go), not from progress
// staleness on the FSM side. The proto reserves field 2 for backward
// audit.
func (fsm *Machine) applyBackupOrder(
	batch *dal.WriteSession,
	raftIndex uint64,
	kind raftcmdpb.BackupKind,
	order *raftcmdpb.BackupOrder,
) error {
	switch op := order.GetOp().(type) {
	case *raftcmdpb.BackupOrder_Start:
		return fsm.dispatchBackupStart(batch, raftIndex, kind, op.Start)
	case *raftcmdpb.BackupOrder_Complete:
		return fsm.dispatchBackupComplete(batch, raftIndex, op.Complete)
	case *raftcmdpb.BackupOrder_Fail:
		return fsm.dispatchBackupFail(batch, raftIndex, op.Fail)
	case nil:
		// An empty oneof reaches the apply path only if the orchestrator
		// emitted a malformed proposal — every builder in this package
		// sets exactly one Op — or a foreign producer skipped the oneof.
		// Per CLAUDE.md invariant #7 the impossible-by-design case fails
		// loudly rather than silently no-op, since a silent skip on the
		// FSM apply path would desync nodes against a hypothetical future
		// producer that did emit such a payload.
		return errors.New("invariant: backup order with nil op (every builder must set exactly one kind)")
	default:
		return fmt.Errorf("backup order: unknown op type %T", op)
	}
}

// applyIncrementalBackupOrder mirrors applyBackupOrder for the
// incremental pipeline. Sharing the carrier shape but branching on the
// order type keeps the proto explicit about which pipeline owns the
// destination slot — no in-band "kind" field to remember on every Start.
func (fsm *Machine) applyIncrementalBackupOrder(
	batch *dal.WriteSession,
	raftIndex uint64,
	order *raftcmdpb.IncrementalBackupOrder,
) error {
	switch op := order.GetOp().(type) {
	case *raftcmdpb.IncrementalBackupOrder_Start:
		return fsm.dispatchBackupStart(batch, raftIndex, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, op.Start)
	case *raftcmdpb.IncrementalBackupOrder_Complete:
		return fsm.dispatchBackupComplete(batch, raftIndex, op.Complete)
	case *raftcmdpb.IncrementalBackupOrder_Fail:
		return fsm.dispatchBackupFail(batch, raftIndex, op.Fail)
	case nil:
		// See applyBackupOrder above — same invariant.
		return errors.New("invariant: incremental backup order with nil op (every builder must set exactly one kind)")
	default:
		return fmt.Errorf("incremental backup order: unknown op type %T", op)
	}
}

func (fsm *Machine) dispatchBackupStart(batch *dal.WriteSession, raftIndex uint64, kind raftcmdpb.BackupKind, start *raftcmdpb.BackupOrderStart) error {
	_, ok, err := fsm.Registry.BackupJobs.Start(batch, raftIndex, kind, start)
	if err != nil {
		// Destination-busy or job_id-collision: business rejections,
		// passed through verbatim. Other errors (Pebble write,
		// marshalling) bubble up as FSM-fatal because the apply cannot
		// proceed in a consistent state.
		if errors.Is(err, ErrBackupInProgress) || errors.Is(err, ErrBackupJobIDCollision) {
			return err
		}

		return err
	}

	if !ok {
		// Defensive: Start returns (job, false, sentinel) when rejected,
		// never (job, false, nil).
		return ErrBackupInProgress
	}

	return nil
}

func (fsm *Machine) dispatchBackupComplete(batch *dal.WriteSession, raftIndex uint64, complete *raftcmdpb.BackupOrderComplete) error {
	if _, err := fsm.Registry.BackupJobs.Complete(batch, raftIndex, complete); err != nil {
		return err
	}

	return nil
}

func (fsm *Machine) dispatchBackupFail(batch *dal.WriteSession, raftIndex uint64, fail *raftcmdpb.BackupOrderFail) error {
	if _, err := fsm.Registry.BackupJobs.Fail(batch, raftIndex, fail); err != nil {
		return err
	}

	return nil
}
