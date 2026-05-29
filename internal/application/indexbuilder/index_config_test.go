package indexbuilder

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestNewLedgerIndexConfig(t *testing.T) {
	t.Parallel()

	cfg := newLedgerIndexConfig()

	require.NotNil(t, cfg)
	assert.NotNil(t, cfg.txMetadataIndexed)
	assert.NotNil(t, cfg.txBuiltinIndexed)
	assert.NotNil(t, cfg.acctMetadataIndexed)
	assert.NotNil(t, cfg.acctBuiltinIndexed)
	assert.NotNil(t, cfg.logBuiltinIndexed)
	assert.Empty(t, cfg.txMetadataIndexed)
	assert.Empty(t, cfg.txBuiltinIndexed)
	assert.Empty(t, cfg.acctMetadataIndexed)
	assert.Empty(t, cfg.acctBuiltinIndexed)
	assert.Empty(t, cfg.logBuiltinIndexed)
}

func TestIsMetadataIndexed(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns false", func(t *testing.T) {
		t.Parallel()

		var cfg *ledgerIndexConfig
		assert.False(t, cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key"))
	})

	t.Run("account metadata indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		cfg.acctMetadataIndexed["role"] = true

		assert.True(t, cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"))
		assert.False(t, cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "other"))
	})

	t.Run("transaction metadata indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		cfg.txMetadataIndexed["category"] = true

		assert.True(t, cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"))
		assert.False(t, cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "other"))
	})

	t.Run("unknown target type returns false", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		assert.False(t, cfg.isMetadataIndexed(commonpb.TargetType(99), "key"))
	})
}

func TestIsBuiltinIndexed(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns false", func(t *testing.T) {
		t.Parallel()

		var cfg *ledgerIndexConfig
		assert.False(t, cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
	})

	t.Run("indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE] = true

		assert.True(t, cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
		assert.False(t, cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP))
	})
}

func TestIsLogBuiltinIndexed(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns false", func(t *testing.T) {
		t.Parallel()

		var cfg *ledgerIndexConfig
		assert.False(t, cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER))
	})

	t.Run("indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER] = true

		assert.True(t, cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER))
		assert.False(t, cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))
	})
}

func TestLedgerConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	// Unknown ledger returns nil.
	assert.Nil(t, b.ledgerConfig("unknown"))

	// Known ledger returns its config.
	cfg := newLedgerIndexConfig()
	b.indexConfig["test"] = cfg
	assert.Same(t, cfg, b.ledgerConfig("test"))
}

func TestGetOrCreateLedgerConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	// First call creates.
	cfg := b.getOrCreateLedgerConfig("test")
	require.NotNil(t, cfg)

	// Second call returns same.
	cfg2 := b.getOrCreateLedgerConfig("test")
	assert.Same(t, cfg, cfg2)
}

func TestHandleCreatedIndexLog_TxBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{
		Index: &commonpb.CreatedIndexLog_Transaction{
			Transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{
					Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
				},
			},
		},
	})

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	// Should have created a backfill task.
	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, "ledger1", b.backfillTasks[0].ledger)
}

func TestHandleCreatedIndexLog_TxMetadata(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{
		Index: &commonpb.CreatedIndexLog_Transaction{
			Transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
			},
		},
	})

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.txMetadataIndexed["category"])
	require.Len(t, b.backfillTasks, 1)
}

func TestHandleCreatedIndexLog_AcctMetadata(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{
		Index: &commonpb.CreatedIndexLog_Account{
			Account: &commonpb.AccountIndex{
				Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: "role"},
			},
		},
	})

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.acctMetadataIndexed["role"])
	require.Len(t, b.backfillTasks, 1)
}

func TestHandleCreatedIndexLog_AcctBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{
		Index: &commonpb.CreatedIndexLog_Account{
			Account: &commonpb.AccountIndex{
				Kind: &commonpb.AccountIndex_Builtin{
					Builtin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED,
				},
			},
		},
	})

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.acctBuiltinIndexed[commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED])
	// No backfill for account builtins yet.
	assert.Empty(t, b.backfillTasks)
}

func TestHandleCreatedIndexLog_LogBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{
		Index: &commonpb.CreatedIndexLog_LogBuiltin{
			LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
		},
	})

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE])
	require.Len(t, b.backfillTasks, 1)
}

// newTestBuilderWithStore creates a Builder backed by a temporary Pebble read store.
// Use this for tests that call removeBackfillTask (which requires readStore.DeleteBackfillProgress).
func newTestBuilderWithStore(t *testing.T) *Builder {
	t.Helper()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	return &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}
}

func TestHandleDroppedIndexLog_TxBuiltin(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	cfg := newLedgerIndexConfig()
	cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE] = true
	b.indexConfig["ledger1"] = cfg

	// Add a backfill task that should be removed.
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	require.Len(t, b.backfillTasks, 1)

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{
		Index: &commonpb.DroppedIndexLog_Transaction{
			Transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_Builtin{
					Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
				},
			},
		},
	})

	assert.False(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.Empty(t, b.backfillTasks)
}

func TestHandleDroppedIndexLog_TxMetadata(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	cfg := newLedgerIndexConfig()
	cfg.txMetadataIndexed["category"] = true
	b.indexConfig["ledger1"] = cfg

	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	require.Len(t, b.backfillTasks, 1)

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{
		Index: &commonpb.DroppedIndexLog_Transaction{
			Transaction: &commonpb.TransactionIndex{
				Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: "category"},
			},
		},
	})

	assert.False(t, cfg.txMetadataIndexed["category"])
	assert.Empty(t, b.backfillTasks)
}

func TestHandleDroppedIndexLog_AcctBuiltin(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	cfg := newLedgerIndexConfig()
	cfg.acctBuiltinIndexed[commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED] = true
	b.indexConfig["ledger1"] = cfg

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{
		Index: &commonpb.DroppedIndexLog_Account{
			Account: &commonpb.AccountIndex{
				Kind: &commonpb.AccountIndex_Builtin{
					Builtin: commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED,
				},
			},
		},
	})

	assert.False(t, cfg.acctBuiltinIndexed[commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED])
}

func TestHandleDroppedIndexLog_AcctMetadata(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	cfg := newLedgerIndexConfig()
	cfg.acctMetadataIndexed["role"] = true
	b.indexConfig["ledger1"] = cfg

	b.addBackfillTaskForAcctMetadata("ledger1", 1, "role")
	require.Len(t, b.backfillTasks, 1)

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{
		Index: &commonpb.DroppedIndexLog_Account{
			Account: &commonpb.AccountIndex{
				Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: "role"},
			},
		},
	})

	assert.False(t, cfg.acctMetadataIndexed["role"])
	assert.Empty(t, b.backfillTasks)
}

func TestHandleDroppedIndexLog_LogBuiltin(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	cfg := newLedgerIndexConfig()
	cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER] = true
	b.indexConfig["ledger1"] = cfg

	b.addBackfillTaskForLogBuiltin("ledger1", 1, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER)
	require.Len(t, b.backfillTasks, 1)

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{
		Index: &commonpb.DroppedIndexLog_LogBuiltin{
			LogBuiltin: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER,
		},
	})

	assert.False(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER])
	assert.Empty(t, b.backfillTasks)
}

func TestAddBackfillTask_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	assert.Len(t, b.backfillTasks, 1)
}

func TestAddBackfillTask_DifferentIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	b.addBackfillTaskForAcctMetadata("ledger1", 1, "role")
	b.addBackfillTaskForLogBuiltin("ledger1", 1, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER)

	assert.Len(t, b.backfillTasks, 5)
}

func TestStripBuildingIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := newLedgerIndexConfig()
	cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE] = true
	cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP] = true
	cfg.txMetadataIndexed["category"] = true
	cfg.acctMetadataIndexed["role"] = true
	cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER] = true
	b.indexConfig["ledger1"] = cfg

	// Add backfill tasks for some of the indexes (simulating BUILDING state).
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	b.addBackfillTaskForAcctMetadata("ledger1", 1, "role")
	b.addBackfillTaskForLogBuiltin("ledger1", 1, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER)

	restore := b.stripBuildingIndexes()

	// BUILDING indexes should be stripped from config.
	assert.False(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.False(t, cfg.txMetadataIndexed["category"])
	assert.False(t, cfg.acctMetadataIndexed["role"])
	assert.False(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER])

	// READY indexes should still be there.
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])

	// Restore should add them back.
	restore()

	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.True(t, cfg.txMetadataIndexed["category"])
	assert.True(t, cfg.acctMetadataIndexed["role"])
	assert.True(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])
}

func TestStripBuildingIndexes_NilConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	// Add a backfill task for a ledger with no config.
	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: "missing",
		index: indexID{transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_Builtin{
				Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			},
		}},
		bbKey: backfillBBKey(99, indexID{transaction: &commonpb.TransactionIndex{
			Kind: &commonpb.TransactionIndex_Builtin{
				Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			},
		}}),
	})

	// Should not panic with nil config.
	restore := b.stripBuildingIndexes()
	restore()
}

func TestLoadLedgerIndexConfig_BuildingTxBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		BuiltinIndexes: &commonpb.BuiltinIndexConfig{
			Reference:       true,
			ReferenceStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
			Timestamp:       true,
			TimestampStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])

	// Only BUILDING should have backfill task.
	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, "ledger1", b.backfillTasks[0].ledger)
}

func TestLoadLedgerIndexConfig_DisabledIndexesNotIncluded(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		BuiltinIndexes: &commonpb.BuiltinIndexConfig{
			Reference:       false,
			Timestamp:       true,
			TimestampStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.False(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])
	assert.Empty(t, b.backfillTasks)
}

func TestLoadLedgerIndexConfig_MetadataIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"role": {
					Indexed:          true,
					IndexBuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
				},
				"status": {
					Indexed:          true,
					IndexBuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
				},
				"ignore": {
					Indexed: false,
				},
			},
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				"category": {
					Indexed:          true,
					IndexBuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
				},
			},
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.acctMetadataIndexed["role"])
	assert.True(t, cfg.acctMetadataIndexed["status"])
	assert.False(t, cfg.acctMetadataIndexed["ignore"])
	assert.True(t, cfg.txMetadataIndexed["category"])

	// BUILDING indexes should have backfill tasks (role + category).
	assert.Len(t, b.backfillTasks, 2)
}

func TestLoadLedgerIndexConfig_LogBuiltinIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		LogBuiltinIndexes: &commonpb.LogBuiltinIndexConfig{
			Ledger:       true,
			LedgerStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
			Date:         true,
			DateStatus:   commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER])
	assert.True(t, cfg.logBuiltinIndexed[commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE])

	require.Len(t, b.backfillTasks, 1)
}

func TestLoadLedgerIndexConfig_AllBuiltinIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		BuiltinIndexes: &commonpb.BuiltinIndexConfig{
			Reference:           true,
			ReferenceStatus:     commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
			Timestamp:           true,
			TimestampStatus:     commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
			Address:             true,
			AddressStatus:       commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
			SourceAddress:       true,
			SourceAddressStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
			DestAddress:         true,
			DestAddressStatus:   commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
			InsertedAt:          true,
			InsertedAtStatus:    commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS])
	assert.True(t, cfg.txBuiltinIndexed[commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT])
	assert.Empty(t, b.backfillTasks)
}

func TestAddSchemaRewriteTask_NotIndexed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := newLedgerIndexConfig()

	b.addSchemaRewriteTask(cfg, 1, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	assert.Empty(t, b.schemaRewriteTasks)
}

func TestAddSchemaRewriteTask_Indexed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := newLedgerIndexConfig()
	cfg.acctMetadataIndexed["status"] = true

	b.addSchemaRewriteTask(cfg, 1, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, uint32(1), b.schemaRewriteTasks[0].ledgerID)
	assert.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, b.schemaRewriteTasks[0].targetType)
	assert.Equal(t, "status", b.schemaRewriteTasks[0].key)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, b.schemaRewriteTasks[0].toType)

	expectedBBKey := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	assert.Equal(t, expectedBBKey, b.schemaRewriteTasks[0].bbKey)
}

func TestAddSchemaRewriteTask_DuplicateResetsProgress(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := newLedgerIndexConfig()
	cfg.acctMetadataIndexed["status"] = true

	b.addSchemaRewriteTask(cfg, 1, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	b.schemaRewriteTasks[0].rmapCursor = []byte("some-cursor")
	b.schemaRewriteTasks[0].processedCount = 100

	b.addSchemaRewriteTask(cfg, 1, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, b.schemaRewriteTasks[0].toType)
	assert.Nil(t, b.schemaRewriteTasks[0].rmapCursor)
	assert.Equal(t, uint64(0), b.schemaRewriteTasks[0].processedCount)
}

func TestAddSchemaRewriteTask_Transaction(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := newLedgerIndexConfig()
	cfg.txMetadataIndexed["tag"] = true

	b.addSchemaRewriteTask(cfg, 1, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		Key:        "tag",
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, commonpb.TargetType_TARGET_TYPE_TRANSACTION, b.schemaRewriteTasks[0].targetType)
}

func TestRemoveSchemaRewriteTask(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	bbKey1 := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key1")
	bbKey2 := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key2")
	bbKey3 := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key3")

	b.schemaRewriteTasks = []*schemaRewriteTask{
		{ledger: "ledger1", key: "key1", bbKey: bbKey1},
		{ledger: "ledger1", key: "key2", bbKey: bbKey2},
		{ledger: "ledger1", key: "key3", bbKey: bbKey3},
	}

	// Remove the middle task.
	b.removeSchemaRewriteTask(1)

	require.Len(t, b.schemaRewriteTasks, 2)
	assert.Equal(t, "key1", b.schemaRewriteTasks[0].key)
	assert.Equal(t, "key3", b.schemaRewriteTasks[1].key)

	b.removeSchemaRewriteTask(0)
	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, "key3", b.schemaRewriteTasks[0].key)

	b.removeSchemaRewriteTask(0)
	assert.Empty(t, b.schemaRewriteTasks)
}

func TestRemoveBackfillTask(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	require.Len(t, b.backfillTasks, 3)

	b.removeBackfillTask(indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{
			Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
		},
	}})

	assert.Len(t, b.backfillTasks, 2)

	// Remove one that doesn't exist (no-op).
	b.removeBackfillTask(indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{
			Builtin: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
		},
	}})
	assert.Len(t, b.backfillTasks, 2)
}

// noopLogger implements the logging.Logger interface for tests without output.
type noopLogger struct{}

var _ logging.Logger = noopLogger{}

func (noopLogger) Debugf(string, ...any)                        {}
func (noopLogger) Infof(string, ...any)                         {}
func (noopLogger) Errorf(string, ...any)                        {}
func (noopLogger) Debug(...any)                                 {}
func (noopLogger) Info(...any)                                  {}
func (noopLogger) Error(...any)                                 {}
func (n noopLogger) WithFields(map[string]any) logging.Logger   { return n }
func (n noopLogger) WithField(string, any) logging.Logger       { return n }
func (n noopLogger) WithContext(context.Context) logging.Logger { return n }
func (noopLogger) Writer() io.Writer                            { return io.Discard }
func (noopLogger) Enabled(logging.Level) bool                   { return false }
