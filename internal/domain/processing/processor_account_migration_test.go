package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// --- helpers ---

func newLedgerInfoWithTypes(types map[string]*commonpb.AccountType) *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{AccountTypes: types}
}

func activeType(pattern string) *commonpb.AccountType {
	return &commonpb.AccountType{
		Pattern: pattern,
		Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
	}
}

func migratingType(pattern, supersededBy string) *commonpb.AccountType {
	return &commonpb.AccountType{
		Pattern:      pattern,
		Status:       commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING,
		SupersededBy: supersededBy,
		MigrationProgress: &commonpb.MigrationProgress{
			TotalAccounts:    10,
			MigratedAccounts: 0,
		},
	}
}

func migrateOrder(ledger, sourceType, targetType string, totalAccounts uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_MigrateAccountType{
					MigrateAccountType: &raftcmdpb.MigrateAccountTypeOrder{
						SourceType:    sourceType,
						TargetType:    targetType,
						TotalAccounts: totalAccounts,
					},
				},
			},
		},
	}
}

func migrateBatchOrder(ledger, sourceType string, volumeEntries []*raftcmdpb.MigrateAccountEntry, soFar uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_MigrateAccountBatch{
					MigrateAccountBatch: &raftcmdpb.MigrateAccountBatchOrder{
						SourceType:            sourceType,
						VolumeEntries:         volumeEntries,
						MigratedAccountsSoFar: soFar,
					},
				},
			},
		},
	}
}

func completeMigrationOrder(ledger, sourceType string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_CompleteMigration{
					CompleteMigration: &raftcmdpb.CompleteMigrationOrder{
						SourceType: sourceType,
					},
				},
			},
		},
	}
}

func cancelMigrationOrder(ledger, sourceType string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: ledger,
				Data: &raftcmdpb.LedgerApplyOrder_CancelMigration{
					CancelMigration: &raftcmdpb.CancelMigrationOrder{
						SourceType: sourceType,
					},
				},
			},
		},
	}
}

func makeVolumeEntry(oldLedger, oldAccount, oldAsset, newLedger, newAccount, newAsset string) *raftcmdpb.MigrateAccountEntry {
	oldKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: oldLedger, Account: oldAccount},
		Asset:      oldAsset,
	}
	newKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: newLedger, Account: newAccount},
		Asset:      newAsset,
	}

	return &raftcmdpb.MigrateAccountEntry{
		OldCanonicalKey: oldKey.Bytes(),
		NewCanonicalKey: newKey.Bytes(),
	}
}

// --- processMigrateAccountType tests ---

func TestProcessMigrateAccountType_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1000}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"old-type": activeType("users:{id}:old"),
		"new-type": activeType("users:{id}:new"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())
	mockStore.EXPECT().AddAccountMigrationRequest("ledger1", "old-type", "new-type", "users:{id}:old", "users:{id}:new")

	result, err := processor.ProcessOrder(migrateOrder("ledger1", "old-type", "new-type", 42), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	started := applyLog.Log.Data.GetStartedMigration()
	require.NotNil(t, started)
	require.Equal(t, "old-type", started.SourceType)
	require.Equal(t, "new-type", started.TargetType)
	require.Equal(t, "users:{id}:old", started.SourcePattern)
	require.Equal(t, "users:{id}:new", started.TargetPattern)
	require.Equal(t, uint64(42), started.TotalAccounts)

	// Verify source type was mutated to MIGRATING.
	require.Equal(t, commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING, info.AccountTypes["old-type"].Status)
	require.Equal(t, "new-type", info.AccountTypes["old-type"].SupersededBy)
	require.NotNil(t, info.AccountTypes["old-type"].MigrationProgress)
	require.Equal(t, uint64(42), info.AccountTypes["old-type"].MigrationProgress.TotalAccounts)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.AccountTypes["old-type"].EnforcementMode)
}

func TestProcessMigrateAccountType_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("missing", "src", "dst", 0), mockStore)
	require.Error(t, err)

	var ledgerErr *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerErr)
}

func TestProcessMigrateAccountType_SourceTypeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"other": activeType("other:{id}"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("ledger1", "missing-src", "other", 0), mockStore)
	require.Error(t, err)

	var notFoundErr *domain.ErrAccountTypeNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, "missing-src", notFoundErr.Name)
}

func TestProcessMigrateAccountType_TargetTypeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": activeType("users:{id}"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("ledger1", "src", "missing-dst", 0), mockStore)
	require.Error(t, err)

	var notFoundErr *domain.ErrAccountTypeNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, "missing-dst", notFoundErr.Name)
}

func TestProcessMigrateAccountType_SourceNotActive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": {
			Pattern: "users:{id}",
			Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED,
		},
		"dst": activeType("accounts:{id}"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("ledger1", "src", "dst", 0), mockStore)
	require.Error(t, err)

	var migErr *domain.ErrMigrationAlreadyActive
	require.ErrorAs(t, err, &migErr)
	require.Equal(t, "src", migErr.ActiveType)
}

func TestProcessMigrateAccountType_TargetNotActive(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": activeType("users:{id}"),
		"dst": {
			Pattern: "accounts:{id}",
			Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED,
		},
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("ledger1", "src", "dst", 0), mockStore)
	require.Error(t, err)

	var migErr *domain.ErrMigrationAlreadyActive
	require.ErrorAs(t, err, &migErr)
	require.Equal(t, "dst", migErr.ActiveType)
}

func TestProcessMigrateAccountType_AnotherTypeMigrating(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src":     activeType("users:{id}"),
		"dst":     activeType("accounts:{id}"),
		"already": migratingType("old:{id}", "somewhere"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateOrder("ledger1", "src", "dst", 0), mockStore)
	require.Error(t, err)

	var migErr *domain.ErrMigrationAlreadyActive
	require.ErrorAs(t, err, &migErr)
	require.Equal(t, "already", migErr.ActiveType)
}

// --- processMigrateAccountBatch tests ---

func TestProcessMigrateAccountBatch_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": migratingType("users:{id}:old", "dst"),
		"dst": activeType("users:{id}:new"),
	})

	oldKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:old"},
		Asset:      "USD",
	}
	newKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:new"},
		Asset:      "USD",
	}
	volumeVal := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100), OutputKnown: commonpb.NewUint256FromUint64(50)}

	entries := []*raftcmdpb.MigrateAccountEntry{
		makeVolumeEntry("ledger1", "users:alice:old", "USD", "ledger1", "users:alice:new", "USD"),
	}

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 2000}).AnyTimes()
	mockStore.EXPECT().GetVolume(oldKey).Return(volumeVal, nil)
	mockStore.EXPECT().PutVolume(newKey, volumeVal)
	mockStore.EXPECT().DeleteVolume(oldKey)
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	result, err := processor.ProcessOrder(migrateBatchOrder("ledger1", "src", entries, 5), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	batch := result.GetApply().Log.Data.GetMigratedAccountBatch()
	require.NotNil(t, batch)
	require.Equal(t, "src", batch.SourceType)
	require.Equal(t, uint64(5), batch.MigratedAccountsSoFar)
	require.Len(t, batch.Accounts, 1)
	require.Equal(t, "users:alice:old", batch.Accounts[0].OldAddress)
	require.Equal(t, "users:alice:new", batch.Accounts[0].NewAddress)

	// Verify progress was updated.
	require.Equal(t, uint64(5), info.AccountTypes["src"].MigrationProgress.MigratedAccounts)
}

func TestProcessMigrateAccountBatch_MultipleEntriesSameAccount(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": migratingType("users:{id}:old", "dst"),
		"dst": activeType("users:{id}:new"),
	})

	// Same account, two assets.
	entries := []*raftcmdpb.MigrateAccountEntry{
		makeVolumeEntry("ledger1", "users:alice:old", "USD", "ledger1", "users:alice:new", "USD"),
		makeVolumeEntry("ledger1", "users:alice:old", "EUR", "ledger1", "users:alice:new", "EUR"),
	}

	oldKeyUSD := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:old"}, Asset: "USD"}
	newKeyUSD := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:new"}, Asset: "USD"}
	oldKeyEUR := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:old"}, Asset: "EUR"}
	newKeyEUR := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:new"}, Asset: "EUR"}

	volUSD := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100), OutputKnown: commonpb.NewUint256FromUint64(50)}
	volEUR := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(200), OutputKnown: commonpb.NewUint256FromUint64(100)}

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 2000}).AnyTimes()
	mockStore.EXPECT().GetVolume(oldKeyUSD).Return(volUSD, nil)
	mockStore.EXPECT().PutVolume(newKeyUSD, volUSD)
	mockStore.EXPECT().DeleteVolume(oldKeyUSD)
	mockStore.EXPECT().GetVolume(oldKeyEUR).Return(volEUR, nil)
	mockStore.EXPECT().PutVolume(newKeyEUR, volEUR)
	mockStore.EXPECT().DeleteVolume(oldKeyEUR)
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	result, err := processor.ProcessOrder(migrateBatchOrder("ledger1", "src", entries, 3), mockStore)
	require.NoError(t, err)

	batch := result.GetApply().Log.Data.GetMigratedAccountBatch()
	require.NotNil(t, batch)
	// Two volume entries but same account → deduped to 1 migrated account.
	require.Len(t, batch.Accounts, 1)
	require.Equal(t, "users:alice:old", batch.Accounts[0].OldAddress)
}

func TestProcessMigrateAccountBatch_StaleBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	// Source is no longer MIGRATING (was cancelled).
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": activeType("users:{id}:old"),
	})

	entries := []*raftcmdpb.MigrateAccountEntry{
		makeVolumeEntry("ledger1", "users:alice:old", "USD", "ledger1", "users:alice:new", "USD"),
	}

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 2000}).AnyTimes()
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())
	// No GetVolume/PutVolume/DeleteVolume calls — batch is silently ignored.

	result, err := processor.ProcessOrder(migrateBatchOrder("ledger1", "src", entries, 5), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	batch := result.GetApply().Log.Data.GetMigratedAccountBatch()
	require.NotNil(t, batch)
	require.Equal(t, "src", batch.SourceType)
	// No accounts in the log since nothing was actually migrated.
	require.Empty(t, batch.Accounts)
}

func TestProcessMigrateAccountBatch_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	_, err = processor.ProcessOrder(migrateBatchOrder("missing", "src", nil, 0), mockStore)
	require.Error(t, err)

	var ledgerErr *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerErr)
}

func TestProcessMigrateAccountBatch_SourceTypeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(migrateBatchOrder("ledger1", "missing", nil, 0), mockStore)
	require.Error(t, err)

	var notFoundErr *domain.ErrAccountTypeNotFound
	require.ErrorAs(t, err, &notFoundErr)
}

func TestProcessMigrateAccountBatch_AlreadyMigratedKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": migratingType("users:{id}:old", "dst"),
		"dst": activeType("users:{id}:new"),
	})

	entries := []*raftcmdpb.MigrateAccountEntry{
		makeVolumeEntry("ledger1", "users:alice:old", "USD", "ledger1", "users:alice:new", "USD"),
	}

	oldKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: "ledger1", Account: "users:alice:old"}, Asset: "USD"}

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 2000}).AnyTimes()
	// Key already migrated — GetVolume returns error.
	mockStore.EXPECT().GetVolume(oldKey).Return(nil, domain.ErrNotFound)
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	result, err := processor.ProcessOrder(migrateBatchOrder("ledger1", "src", entries, 1), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	batch := result.GetApply().Log.Data.GetMigratedAccountBatch()
	require.NotNil(t, batch)
	// Account still appears in log since the entry was in the batch.
	require.Len(t, batch.Accounts, 1)
}

// --- processCompleteMigration tests ---

func TestProcessCompleteMigration_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 3000}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": migratingType("users:{id}:old", "dst"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	result, err := processor.ProcessOrder(completeMigrationOrder("ledger1", "src"), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	completed := result.GetApply().Log.Data.GetCompletedMigration()
	require.NotNil(t, completed)
	require.Equal(t, "src", completed.SourceType)

	// Verify source type was set to DEPRECATED.
	require.Equal(t, commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED, info.AccountTypes["src"].Status)
	require.NotNil(t, info.AccountTypes["src"].MigrationProgress.CompletedAt)
	require.Equal(t, uint64(3000), info.AccountTypes["src"].MigrationProgress.CompletedAt.Data)
}

func TestProcessCompleteMigration_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	_, err = processor.ProcessOrder(completeMigrationOrder("missing", "src"), mockStore)
	require.Error(t, err)

	var ledgerErr *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerErr)
}

func TestProcessCompleteMigration_SourceTypeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(completeMigrationOrder("ledger1", "missing"), mockStore)
	require.Error(t, err)

	var notFoundErr *domain.ErrAccountTypeNotFound
	require.ErrorAs(t, err, &notFoundErr)
}

func TestProcessCompleteMigration_NilMigrationProgress(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	// Type exists but has no MigrationProgress (edge case).
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": activeType("users:{id}"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 4000}).AnyTimes()
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	// Should not panic — MigrationProgress is nil, completedAt write is skipped.
	result, err := processor.ProcessOrder(completeMigrationOrder("ledger1", "src"), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED, info.AccountTypes["src"].Status)
}

// --- processCancelMigration tests ---

func TestProcessCancelMigration_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{
		"src": migratingType("users:{id}:old", "dst"),
	})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()
	mockStore.EXPECT().GetDate().Return(&commonpb.Timestamp{Data: 5000}).AnyTimes()
	mockStore.EXPECT().PutLedger("ledger1", gomock.Any())
	mockStore.EXPECT().PutBoundaries("ledger1", gomock.Any())

	result, err := processor.ProcessOrder(cancelMigrationOrder("ledger1", "src"), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	cancelled := result.GetApply().Log.Data.GetCancelledMigration()
	require.NotNil(t, cancelled)
	require.Equal(t, "src", cancelled.SourceType)

	// Verify source type was reset to ACTIVE.
	require.Equal(t, commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE, info.AccountTypes["src"].Status)
	require.Empty(t, info.AccountTypes["src"].SupersededBy)
	require.Nil(t, info.AccountTypes["src"].MigrationProgress)
}

func TestProcessCancelMigration_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	mockStore.EXPECT().GetBoundaries("missing").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("missing").Return(nil, false).AnyTimes()

	_, err = processor.ProcessOrder(cancelMigrationOrder("missing", "src"), mockStore)
	require.Error(t, err)

	var ledgerErr *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerErr)
}

func TestProcessCancelMigration_SourceTypeNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	info := newLedgerInfoWithTypes(map[string]*commonpb.AccountType{})

	mockStore.EXPECT().GetBoundaries("ledger1").Return(boundaries, true)
	mockStore.EXPECT().GetLedger("ledger1").Return(info, true).AnyTimes()

	_, err = processor.ProcessOrder(cancelMigrationOrder("ledger1", "missing"), mockStore)
	require.Error(t, err)

	var notFoundErr *domain.ErrAccountTypeNotFound
	require.ErrorAs(t, err, &notFoundErr)
}
