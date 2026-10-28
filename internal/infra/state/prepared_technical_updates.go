package state

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// preparedTechnicalUpdates lives only for one applyProposal call. Deferred
// functions borrow the immutable proposal, which remains owned by that call
// until publication or rejection. They must never escape into a prepared batch.
type preparedTechnicalUpdates struct {
	effects []func(*dal.WriteSession) error
	configs []*commonpb.ClusterConfig
}

// prepareTechnicalUpdates validates scopes and backup transitions without
// touching live state or the shared batch. MirrorSync joins the existing order
// overlay. Direct effects are published only after all orders and cross-order
// validations succeed, so every soft/replay exit structurally discards them.
func (fsm *Machine) prepareTechnicalUpdates(factory processing.ScopeFactory, raftIndex uint64, proposal *raftcmdpb.Proposal) (*preparedTechnicalUpdates, error) {
	if len(proposal.GetTechnicalUpdates()) == 0 {
		return nil, nil
	}

	prepared := &preparedTechnicalUpdates{}
	auditKeyInstalled := fsm.State.AuditKey != ""
	var backups *BackupJobsPreparation

	for i, tu := range proposal.GetTechnicalUpdates() {
		scope, err := factory.NewScope(tu.GetCoverageBits())
		if err != nil {
			return nil, fmt.Errorf("building scope for technical_updates[%d]: %w", i, err)
		}

		switch kind := tu.GetKind().(type) {
		case *raftcmdpb.TechnicalUpdate_AuditKey:
			if len(kind.AuditKey) != 32 {
				return nil, fmt.Errorf("technical_updates[%d]: invalid audit key length %d", i, len(kind.AuditKey))
			}
			if !auditKeyInstalled {
				if fsm.State.NextAuditSequenceID != 1 {
					return nil, fmt.Errorf("technical_updates[%d]: audit key initialized after audit history", i)
				}
				auditKeyInstalled = true
				prepared.effects = append(prepared.effects, func(batch *dal.WriteSession) error {
					if err := batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, kind.AuditKey); err != nil {
						return fmt.Errorf("persisting audit key: %w", err)
					}
					fsm.State.InstallAuditKey(kind.AuditKey)

					return nil
				})
			}
		case *raftcmdpb.TechnicalUpdate_ClusterConfig:
			prepared.configs = append(prepared.configs, kind.ClusterConfig)
		case *raftcmdpb.TechnicalUpdate_EventsSink:
			prepared.effects = append(prepared.effects, func(batch *dal.WriteSession) error {
				return fsm.applyEventsSinkUpdate(batch, kind.EventsSink)
			})
		case *raftcmdpb.TechnicalUpdate_MirrorSync:
			if err := fsm.applyMirrorSyncUpdate(scope, fsm.writeSet, kind.MirrorSync); err != nil {
				return nil, fmt.Errorf("preparing technical_updates[%d] mirror sync: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_IdempotencyEviction:
			prepared.effects = append(prepared.effects, func(batch *dal.WriteSession) error {
				return fsm.applyIdempotencyEviction(batch, kind.IdempotencyEviction)
			})
		case *raftcmdpb.TechnicalUpdate_BackupOrder:
			if backups == nil {
				backups = fsm.Registry.BackupJobs.Prepare()
			}
			if err := prepareFullBackup(backups, raftIndex, kind.BackupOrder); err != nil {
				return nil, fmt.Errorf("preparing technical_updates[%d] backup: %w", i, err)
			}
			prepared.effects = append(prepared.effects, func(batch *dal.WriteSession) error {
				return fsm.applyBackupOrder(batch, raftIndex, raftcmdpb.BackupKind_BACKUP_KIND_FULL, kind.BackupOrder)
			})
		case *raftcmdpb.TechnicalUpdate_IncrementalBackupOrder:
			if backups == nil {
				backups = fsm.Registry.BackupJobs.Prepare()
			}
			if err := prepareIncrementalBackup(backups, raftIndex, kind.IncrementalBackupOrder); err != nil {
				return nil, fmt.Errorf("preparing technical_updates[%d] incremental backup: %w", i, err)
			}
			prepared.effects = append(prepared.effects, func(batch *dal.WriteSession) error {
				return fsm.applyIncrementalBackupOrder(batch, raftIndex, kind.IncrementalBackupOrder)
			})
		default:
			return nil, fmt.Errorf("technical_updates[%d]: unsupported kind %T", i, kind)
		}
	}

	return prepared, nil
}

func prepareFullBackup(backups *BackupJobsPreparation, raftIndex uint64, order *raftcmdpb.BackupOrder) error {
	switch op := order.GetOp().(type) {
	case *raftcmdpb.BackupOrder_Start:
		_, _, err := backups.Start(raftIndex, raftcmdpb.BackupKind_BACKUP_KIND_FULL, op.Start)

		return err
	case *raftcmdpb.BackupOrder_Complete:
		_, err := backups.Complete(raftIndex, op.Complete)

		return err
	case *raftcmdpb.BackupOrder_Fail:
		_, err := backups.Fail(raftIndex, op.Fail)

		return err
	default:
		return fmt.Errorf("invariant: unsupported backup op %T", op)
	}
}

func prepareIncrementalBackup(backups *BackupJobsPreparation, raftIndex uint64, order *raftcmdpb.IncrementalBackupOrder) error {
	switch op := order.GetOp().(type) {
	case *raftcmdpb.IncrementalBackupOrder_Start:
		_, _, err := backups.Start(raftIndex, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL, op.Start)

		return err
	case *raftcmdpb.IncrementalBackupOrder_Complete:
		_, err := backups.Complete(raftIndex, op.Complete)

		return err
	case *raftcmdpb.IncrementalBackupOrder_Fail:
		_, err := backups.Fail(raftIndex, op.Fail)

		return err
	default:
		return fmt.Errorf("invariant: unsupported incremental backup op %T", op)
	}
}

func (p *preparedTechnicalUpdates) publishEffects(batch *dal.WriteSession) error {
	if p != nil {
		for _, apply := range p.effects {
			if err := apply(batch); err != nil {
				return fmt.Errorf("publishing prepared technical update: %w", err)
			}
		}
	}

	return nil
}

// mergeProposalWrites preserves the legacy Merge exactly when p is nil.
// Config resets follow Merge because DerivedKeyStore needs the live parent's
// old values and tombstones to account for business updates and deletions.
// Publication/Merge failures remain fatal: live mutations may already exist,
// and the caller cancels the uncommitted batch; no I/O rollback is promised.
func (fsm *Machine) mergeProposalWrites(batch *dal.WriteSession, raftIndex uint64, logs []*raftcmdpb.CreatedLogOrReference, p *preparedTechnicalUpdates) error {
	if err := p.publishEffects(batch); err != nil {
		return err
	}
	if err := fsm.writeSet.Merge(batch, logs); err != nil {
		return err
	}
	if p != nil {
		for _, cfg := range p.configs {
			if err := fsm.applyClusterConfig(batch, raftIndex, cfg); err != nil {
				return fmt.Errorf("publishing prepared cluster config: %w", err)
			}
		}
	}

	return nil
}
