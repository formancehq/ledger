package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// processMigrateAccountType starts a migration from source type to target type.
func (p *RequestProcessor) processMigrateAccountType(
	ledgerName string,
	order *raftcmdpb.MigrateAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	source, exists := info.AccountTypes[order.SourceType]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.SourceType}
	}

	if source.Status != commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE {
		return nil, &domain.ErrMigrationAlreadyActive{ActiveType: order.SourceType}
	}

	target, exists := info.AccountTypes[order.TargetType]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.TargetType}
	}

	if target.Status != commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE {
		return nil, &domain.ErrMigrationAlreadyActive{ActiveType: order.TargetType}
	}

	// Check no other type is currently MIGRATING.
	for name, at := range info.AccountTypes {
		if name != order.SourceType && at.Status == commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING {
			return nil, &domain.ErrMigrationAlreadyActive{ActiveType: name}
		}
	}

	now := s.GetDate()

	source.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING
	source.SupersededBy = order.TargetType
	source.MigrationProgress = &commonpb.MigrationProgress{
		TotalAccounts:    order.TotalAccounts,
		MigratedAccounts: 0,
		StartedAt:        now,
	}
	// Set source to AUDIT during migration to avoid rejecting transactions using old addresses.
	source.EnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_StartedMigration{
			StartedMigration: &commonpb.StartedMigrationLog{
				SourceType:    order.SourceType,
				TargetType:    order.TargetType,
				SourcePattern: source.Pattern,
				TargetPattern: target.Pattern,
				TotalAccounts: order.TotalAccounts,
			},
		},
	}, nil
}

// processMigrateAccountBatch processes a batch of account key rewrites.
// The actual Pebble key reads/writes happen in the buffer/state layer;
// here we only update migration progress in LedgerInfo.
func (p *RequestProcessor) processMigrateAccountBatch(
	ledgerName string,
	order *raftcmdpb.MigrateAccountBatchOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	source, exists := info.AccountTypes[order.SourceType]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.SourceType}
	}

	if source.Status != commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING {
		// Stale batch — migration was cancelled. Silently ignore.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_MigratedAccountBatch{
				MigratedAccountBatch: &commonpb.MigratedAccountBatchLog{
					SourceType:             order.SourceType,
					MigratedAccountsSoFar:  order.MigratedAccountsSoFar,
				},
			},
		}, nil
	}

	source.MigrationProgress.MigratedAccounts = order.MigratedAccountsSoFar
	s.PutLedger(ledgerName, info)

	// Build migrated account list for the log.
	accounts := make([]*commonpb.MigratedAccount, 0, len(order.VolumeEntries))
	// Volume entries carry the distinct account rewrites.
	// We don't have address info at this layer — the log records canonical keys.
	// The index builder will interpret MigrateAccountEntry keys.

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_MigratedAccountBatch{
			MigratedAccountBatch: &commonpb.MigratedAccountBatchLog{
				SourceType:            order.SourceType,
				Accounts:              accounts,
				MigratedAccountsSoFar: order.MigratedAccountsSoFar,
			},
		},
	}, nil
}

// processCompleteMigration marks a migration as complete.
func (p *RequestProcessor) processCompleteMigration(
	ledgerName string,
	order *raftcmdpb.CompleteMigrationOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	source, exists := info.AccountTypes[order.SourceType]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.SourceType}
	}

	source.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED
	if source.MigrationProgress != nil {
		source.MigrationProgress.CompletedAt = s.GetDate()
	}
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CompletedMigration{
			CompletedMigration: &commonpb.CompletedMigrationLog{
				SourceType: order.SourceType,
			},
		},
	}, nil
}

// processCancelMigration cancels an in-progress migration.
func (p *RequestProcessor) processCancelMigration(
	ledgerName string,
	order *raftcmdpb.CancelMigrationOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	source, exists := info.AccountTypes[order.SourceType]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.SourceType}
	}

	source.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE
	source.SupersededBy = ""
	source.MigrationProgress = nil
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CancelledMigration{
			CancelledMigration: &commonpb.CancelledMigrationLog{
				SourceType: order.SourceType,
			},
		},
	}, nil
}
