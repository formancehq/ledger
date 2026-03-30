package processing

import (
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// processAddAccountType adds a new account type to a ledger.
func (p *RequestProcessor) processAddAccountType(
	ledgerName string,
	order *raftcmdpb.AddAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at := order.GetAccountType()
	if at == nil || at.GetName() == "" {
		return nil, &domain.ErrInvalidPattern{Pattern: "", Details: "account type name is required"}
	}

	if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
	}

	if info.AccountTypes == nil {
		info.AccountTypes = make(map[string]*commonpb.AccountType)
	}

	if _, exists := info.GetAccountTypes()[at.GetName()]; exists {
		return nil, &domain.ErrAccountTypeAlreadyExists{Name: at.GetName()}
	}

	// Set default status to ACTIVE.
	at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE

	info.AccountTypes[at.GetName()] = at
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: at,
			},
		},
	}, nil
}

// processRemoveAccountType removes an account type from a ledger.
// The type must be DEPRECATED or have no matching accounts.
func (p *RequestProcessor) processRemoveAccountType(
	ledgerName string,
	order *raftcmdpb.RemoveAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	if _, exists := info.GetAccountTypes()[order.GetName()]; !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetName()}
	}

	delete(info.GetAccountTypes(), order.GetName())
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
			RemovedAccountType: &commonpb.RemovedAccountTypeLog{
				Name: order.GetName(),
			},
		},
	}, nil
}

// processStartAccountMigration initiates a pattern migration on an account type.
func (p *RequestProcessor) processStartAccountMigration(
	ledgerName string,
	order *raftcmdpb.StartAccountMigrationOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at, exists := info.GetAccountTypes()[order.GetAccountTypeName()]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetAccountTypeName()}
	}

	if at.GetStatus() != commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE {
		return nil, &domain.ErrAccountTypeMigrationInProgress{Name: order.GetAccountTypeName()}
	}

	if err := accounttype.ValidatePattern(order.GetTargetPattern()); err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: order.GetTargetPattern(), Details: err.Error()}
	}

	sourceSegments, err := accounttype.ParsePattern(at.GetPattern())
	if err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
	}

	targetSegments, err := accounttype.ParsePattern(order.GetTargetPattern())
	if err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: order.GetTargetPattern(), Details: err.Error()}
	}

	if err := accounttype.ValidateMigrationCompatible(sourceSegments, targetSegments); err != nil {
		return nil, &domain.ErrAccountTypeMigrationNotCompatible{
			Source:  at.GetPattern(),
			Target:  order.GetTargetPattern(),
			Details: err.Error(),
		}
	}

	oldPattern := at.GetPattern()
	at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING
	at.Migration = &commonpb.AccountTypeMigration{
		TargetPattern:    order.GetTargetPattern(),
		Cursor:           "",
		MigratedAccounts: 0,
	}

	s.PutLedger(ledgerName, info)
	s.AddAccountMigrateRequest(ledgerName, at.GetName(), oldPattern, order.GetTargetPattern())

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_StartedAccountMigration{
			StartedAccountMigration: &commonpb.StartedAccountMigrationLog{
				AccountTypeName: at.GetName(),
				OldPattern:      oldPattern,
				TargetPattern:   order.GetTargetPattern(),
			},
		},
	}, nil
}

// processAccountMigrationBatch processes a batch of account migrations.
func (p *RequestProcessor) processAccountMigrationBatch(
	ledgerName string,
	order *raftcmdpb.AccountMigrationBatchOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at, exists := info.GetAccountTypes()[order.GetAccountTypeName()]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetAccountTypeName()}
	}

	// Staleness check: type must still be MIGRATING with the expected old pattern.
	if at.GetStatus() != commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING ||
		at.GetPattern() != order.GetExpectedOldPattern() {
		// Stale batch: return a no-op log.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_AccountMigrationBatch{
				AccountMigrationBatch: &commonpb.AccountMigrationBatchLog{
					AccountTypeName: order.GetAccountTypeName(),
					Count:           0,
				},
			},
		}, nil
	}

	migration := at.GetMigration()
	zeroVolume := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256(new(uint256.Int)),
		Output: commonpb.NewUint256(new(uint256.Int)),
	}

	var count uint32

	for _, entry := range order.GetEntries() {
		oldAddr := entry.GetOldAddress()
		newAddr := entry.GetNewAddress()

		// Migrate volumes: read old, read new, combine into new, zero old.
		for _, asset := range entry.GetAssets() {
			oldKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: oldAddr},
				Asset:      asset,
			}
			newKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: newAddr},
				Asset:      asset,
			}

			oldVol, err := s.GetVolume(oldKey)
			if err != nil {
				continue
			}

			newVol, err := s.GetVolume(newKey)
			if err != nil {
				// New account has no volume yet; start from zero.
				newVol = &raftcmdpb.VolumePair{
					Input:  commonpb.NewUint256(new(uint256.Int)),
					Output: commonpb.NewUint256(new(uint256.Int)),
				}
			}

			// Combine: new.input += old.input, new.output += old.output.
			var (
				oldInput, oldOutput uint256.Int
				newInput, newOutput uint256.Int
			)

			oldVol.GetInput().IntoUint256(&oldInput)
			oldVol.GetOutput().IntoUint256(&oldOutput)
			newVol.GetInput().IntoUint256(&newInput)
			newVol.GetOutput().IntoUint256(&newOutput)

			combinedInput := new(uint256.Int).Add(&newInput, &oldInput)
			combinedOutput := new(uint256.Int).Add(&newOutput, &oldOutput)

			s.PutVolume(newKey, &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256(combinedInput),
				Output: commonpb.NewUint256(combinedOutput),
			})
			s.PutVolume(oldKey, zeroVolume)
		}

		// Migrate metadata: read old, put on new, delete old.
		for _, key := range entry.GetMetadataKeys() {
			oldMK := domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: oldAddr},
				Key:        key,
			}
			newMK := domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledgerName, Account: newAddr},
				Key:        key,
			}

			value, err := s.GetAccountMetadata(oldMK)
			if err != nil {
				continue
			}

			s.PutAccountMetadata(newMK, value)
			s.DeleteAccountMetadata(oldMK)
		}

		count++
	}

	// Update migration progress.
	if len(order.GetEntries()) > 0 {
		lastEntry := order.GetEntries()[len(order.GetEntries())-1]
		migration.Cursor = lastEntry.GetOldAddress()
	}

	migration.MigratedAccounts += uint64(count)

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AccountMigrationBatch{
			AccountMigrationBatch: &commonpb.AccountMigrationBatchLog{
				AccountTypeName: order.GetAccountTypeName(),
				Count:           count,
			},
		},
	}, nil
}

// processCompleteAccountMigration finalizes a pattern migration.
func (p *RequestProcessor) processCompleteAccountMigration(
	ledgerName string,
	order *raftcmdpb.CompleteAccountMigrationOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at, exists := info.GetAccountTypes()[order.GetAccountTypeName()]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetAccountTypeName()}
	}

	// Staleness check: type must still be MIGRATING with the expected old pattern.
	if at.GetStatus() != commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING ||
		at.GetPattern() != order.GetExpectedOldPattern() {
		// Stale completion: return a no-op log.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CompletedAccountMigration{
				CompletedAccountMigration: &commonpb.CompletedAccountMigrationLog{
					AccountTypeName: order.GetAccountTypeName(),
					OldPattern:      order.GetExpectedOldPattern(),
					NewPattern:      "",
					TotalMigrated:   0,
				},
			},
		}, nil
	}

	migration := at.GetMigration()
	oldPattern := at.GetPattern()

	at.Pattern = migration.GetTargetPattern()
	at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE
	totalMigrated := migration.GetMigratedAccounts()
	at.Migration = nil

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CompletedAccountMigration{
			CompletedAccountMigration: &commonpb.CompletedAccountMigrationLog{
				AccountTypeName: at.GetName(),
				OldPattern:      oldPattern,
				NewPattern:      at.GetPattern(),
				TotalMigrated:   totalMigrated,
			},
		},
	}, nil
}

// validatePostingsAgainstAccountTypes validates postings against account types.
// Uses longest-match (highest specificity) to find the best matching type.
// When an address doesn't match any type, defaultMode controls the behavior:
// STRICT rejects the transaction, AUDIT silently allows it.
func validatePostingsAgainstAccountTypes(
	postings []*commonpb.Posting,
	types map[string]*commonpb.AccountType,
	defaultMode commonpb.ChartEnforcementMode,
) error {
	if len(types) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	addresses := make([]string, 0, len(postings)*2)
	for _, posting := range postings {
		addresses = append(addresses, posting.GetSource(), posting.GetDestination())
	}

	for _, address := range addresses {
		if address == "world" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}

		if accounttype.FindMatchingType(address, types) == nil {
			if defaultMode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
				return &domain.ErrAccountNotMatchingType{Address: address}
			}

			continue
		}
	}

	return nil
}

// validateAccountAgainstAccountTypes validates a single account address.
// When the address doesn't match any type, defaultMode controls the behavior.
func validateAccountAgainstAccountTypes(
	address string,
	types map[string]*commonpb.AccountType,
	defaultMode commonpb.ChartEnforcementMode,
) error {
	if len(types) == 0 || address == "world" {
		return nil
	}

	if accounttype.FindMatchingType(address, types) == nil {
		if defaultMode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
			return &domain.ErrAccountNotMatchingType{Address: address}
		}
	}

	return nil
}

// processUpdateDefaultEnforcementMode updates the ledger's default enforcement mode.
func (p *RequestProcessor) processUpdateDefaultEnforcementMode(
	ledgerName string,
	order *raftcmdpb.UpdateDefaultEnforcementModeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	info.DefaultEnforcementMode = order.GetEnforcementMode()
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
			UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{
				EnforcementMode: order.GetEnforcementMode(),
			},
		},
	}, nil
}
