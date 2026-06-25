package indexbuilder

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestNewLedgerIndexConfig(t *testing.T) {
	t.Parallel()

	cfg := newLedgerIndexConfig()

	require.NotNil(t, cfg)
	assert.Empty(t, cfg.byCanonical)
}

func TestIsIndexed(t *testing.T) {
	t.Parallel()

	t.Run("nil config returns false", func(t *testing.T) {
		t.Parallel()

		var cfg *ledgerIndexConfig
		assert.False(t, cfg.isIndexed(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k")))
	})

	t.Run("metadata indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
		cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

		assert.True(t, cfg.isIndexed(id))
		assert.False(t, cfg.isIndexed(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "other")))
	})

	t.Run("tx builtin indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
		cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

		assert.True(t, cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
		assert.False(t, cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP))
	})

	t.Run("log builtin indexed", func(t *testing.T) {
		t.Parallel()

		cfg := newLedgerIndexConfig()
		id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
		cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

		assert.True(t, cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))
	})
}

func TestLedgerConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	assert.Nil(t, b.ledgerConfig("unknown"))

	cfg := newLedgerIndexConfig()
	b.indexConfig["test"] = cfg
	assert.Same(t, cfg, b.ledgerConfig("test"))
}

func TestGetOrCreateLedgerConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	cfg := b.getOrCreateLedgerConfig("test")
	require.NotNil(t, cfg)

	cfg2 := b.getOrCreateLedgerConfig("test")
	assert.Same(t, cfg, cfg2)
}

func TestHandleCreatedIndexLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		id          *commonpb.IndexID
		hasBackfill bool
	}{
		{"tx builtin", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE), true},
		{"tx metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"), true},
		{"acct metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"), true},
		{"acct builtin", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED), false},
		{"log builtin", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

			b.handleCreatedIndexLog("ledger1", &commonpb.CreatedIndexLog{Id: tt.id})

			cfg := b.indexConfig["ledger1"]
			require.NotNil(t, cfg)
			assert.True(t, cfg.isIndexed(tt.id))

			if tt.hasBackfill {
				require.Len(t, b.backfillTasks, 1)
				assert.Equal(t, "ledger1", b.backfillTasks[0].ledger)
			} else {
				assert.Empty(t, b.backfillTasks)
			}
		})
	}
}

func TestHandleDroppedIndexLog(t *testing.T) {
	t.Parallel()

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	b := newTestBuilderWithStore(t)
	b.handleCreatedIndexLog("ledger1", &commonpb.CreatedIndexLog{Id: id})
	require.Len(t, b.backfillTasks, 1)
	assert.True(t, b.indexConfig["ledger1"].isIndexed(id))

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{Id: id})
	assert.False(t, b.indexConfig["ledger1"].isIndexed(id))
	assert.Empty(t, b.backfillTasks)
}

func TestAddBackfillTask_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	assert.Len(t, b.backfillTasks, 1)
}

func TestAddBackfillTask_DifferentIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", "category")
	b.addBackfillTaskForAcctMetadata("ledger1", "role")
	b.addBackfillTaskForLogBuiltin("ledger1", commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	assert.Len(t, b.backfillTasks, 5)
}

func TestStripBuildingIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	refID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	tsID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	catID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")
	roleID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	dateID := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	cfg := newLedgerIndexConfig()
	for _, id := range []*commonpb.IndexID{refID, tsID, catID, roleID, dateID} {
		cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}
	}

	b.indexConfig["ledger1"] = cfg

	// Mark BUILDING for all but tsID.
	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxMetadata("ledger1", "category")
	b.addBackfillTaskForAcctMetadata("ledger1", "role")
	b.addBackfillTaskForLogBuiltin("ledger1", commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	restore := b.stripBuildingIndexes()

	assert.False(t, cfg.isIndexed(refID))
	assert.False(t, cfg.isIndexed(catID))
	assert.False(t, cfg.isIndexed(roleID))
	assert.False(t, cfg.isIndexed(dateID))
	assert.True(t, cfg.isIndexed(tsID))

	restore()

	assert.True(t, cfg.isIndexed(refID))
	assert.True(t, cfg.isIndexed(catID))
	assert.True(t, cfg.isIndexed(roleID))
	assert.True(t, cfg.isIndexed(dateID))
	assert.True(t, cfg.isIndexed(tsID))
}

func TestStripBuildingIndexes_NilConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: "missing",
		index:  id,
		bbKey:  backfillBBKey("ledger1", id),
	})

	restore := b.stripBuildingIndexes()
	restore()
}

// TestScheduleBackfillForIndex_OnlyBuilding pins the per-index dispatch
// rule scheduleBackfillForIndex enforces: every kind we recognise plugs
// into its matching backfill scheduler, and READY entries are filtered
// upstream by the caller (here the test mirrors that filter explicitly).
// This is the unit-level contract; TestLoadIndexRegistry_StreamsAndDispatches
// covers the end-to-end Pebble scan path.
func TestScheduleBackfillForIndex_OnlyBuilding(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.indexConfig["ledger1"] = newLedgerIndexConfig()

	refID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	tsID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	roleID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	categoryID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")
	dateID := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	cfg := b.indexConfig["ledger1"]
	cfg.byCanonical[indexes.Canonical(refID)] = &commonpb.Index{Id: refID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, Ledger: "ledger1"}
	cfg.byCanonical[indexes.Canonical(tsID)] = &commonpb.Index{Id: tsID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY, Ledger: "ledger1"}
	cfg.byCanonical[indexes.Canonical(roleID)] = &commonpb.Index{Id: roleID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, Ledger: "ledger1"}
	cfg.byCanonical[indexes.Canonical(categoryID)] = &commonpb.Index{Id: categoryID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, Ledger: "ledger1"}
	cfg.byCanonical[indexes.Canonical(dateID)] = &commonpb.Index{Id: dateID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING, Ledger: "ledger1"}

	for _, idx := range cfg.byCanonical {
		if idx.GetBuildStatus() != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			continue
		}

		b.scheduleBackfillForIndex("ledger1", idx.GetId())
	}

	assert.True(t, cfg.isIndexed(refID))
	assert.True(t, cfg.isIndexed(tsID))
	assert.True(t, cfg.isIndexed(roleID))
	assert.True(t, cfg.isIndexed(categoryID))
	assert.True(t, cfg.isIndexed(dateID))

	// Only BUILDING entries trigger backfill scheduling (4 of the 5 above).
	assert.Len(t, b.backfillTasks, 4)
}

// TestLoadIndexRegistry_StreamsAndDispatches drives the SubAttrIndex scan
// end-to-end: real Index rows are persisted into Pebble via attrs.Index.Set
// and loadIndexRegistry is invoked against a fresh ReadHandle. The test pins
// every dispatch branch the loader implements:
//   - per-ledger entries land in the matching indexConfig.byCanonical
//   - bucket-scope entries (Ledger == "") land in bucketIndexConfig
//   - BUILDING entries schedule a backfill, READY entries don't
//   - orphan rows whose Ledger has no indexConfig are dropped silently
func TestLoadIndexRegistry_StreamsAndDispatches(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.indexConfig["ledgerA"] = newLedgerIndexConfig()
	b.indexConfig["ledgerB"] = newLedgerIndexConfig()

	refID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	roleID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	categoryID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")
	dateID := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	orphanID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)

	type seed struct {
		ledger string
		id     *commonpb.IndexID
		status commonpb.IndexBuildStatus
	}
	seeds := []seed{
		{"ledgerA", refID, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
		{"ledgerA", roleID, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY},
		{"ledgerB", categoryID, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
		{"", dateID, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},        // bucket-scope
		{"ghost", orphanID, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING}, // orphan: no indexConfig entry
	}

	fsmBatch := b.pebbleStore.OpenWriteSession()
	for _, s := range seeds {
		k := domain.IndexKey{LedgerName: s.ledger, Canonical: indexes.Canonical(s.id)}.Bytes()
		_, err := b.attrs.Index.Set(fsmBatch, k, &commonpb.Index{
			Ledger:      s.ledger,
			Id:          s.id,
			BuildStatus: s.status,
		})
		require.NoError(t, err)
	}
	require.NoError(t, fsmBatch.Commit())

	handle, err := b.pebbleStore.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	require.NoError(t, b.loadIndexRegistry(handle))

	// Per-ledger dispatch + status preserved.
	assert.True(t, b.indexConfig["ledgerA"].isIndexed(refID))
	assert.True(t, b.indexConfig["ledgerA"].isIndexed(roleID))
	assert.True(t, b.indexConfig["ledgerB"].isIndexed(categoryID))

	// Bucket-scope landed in bucketIndexConfig, not in any per-ledger map.
	require.NotNil(t, b.bucketIndexConfig)
	assert.True(t, b.bucketIndexConfig.isIndexed(dateID))

	// Orphan row (no indexConfig entry for "ghost") was dropped silently.
	_, hasGhost := b.indexConfig["ghost"]
	assert.False(t, hasGhost, "orphan ledger must not be implicitly created")

	// Only BUILDING per-ledger entries scheduled backfills — bucket-scope and
	// orphan rows never reach scheduleBackfillForIndex.
	require.Len(t, b.backfillTasks, 2)
	scheduledLedgers := map[string]struct{}{}
	for _, task := range b.backfillTasks {
		scheduledLedgers[task.ledger] = struct{}{}
	}
	assert.Contains(t, scheduledLedgers, "ledgerA")
	assert.Contains(t, scheduledLedgers, "ledgerB")
}

func TestRemoveBackfillTask(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", "category")
	require.Len(t, b.backfillTasks, 3)

	b.removeBackfillTask("ledger1", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP))
	assert.Len(t, b.backfillTasks, 2)

	// Removing one that doesn't exist is a no-op.
	b.removeBackfillTask("ledger1", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT))
	assert.Len(t, b.backfillTasks, 2)
}

// TestRemoveBackfillTask_ScopedByLedger guards against the cross-ledger
// hazard: when two ledgers have a backfill running on the same IndexID
// (same metadata key indexed on both), removing the task on one ledger
// must not touch the other's task.
func TestRemoveBackfillTask_ScopedByLedger(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	b.addBackfillTaskForAcctMetadata("ledger1", "score")
	b.addBackfillTaskForAcctMetadata("ledger2", "score")
	require.Len(t, b.backfillTasks, 2)

	b.removeBackfillTask("ledger1", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score"))

	require.Len(t, b.backfillTasks, 1, "the other ledger's task must survive")
	assert.Equal(t, "ledger2", b.backfillTasks[0].ledger,
		"removeBackfillTask must filter by ledger, not just IndexID")
}

func TestAddSchemaRewriteTask_NotIndexed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	cfg := newLedgerIndexConfig()

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
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
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, "test-ledger", b.schemaRewriteTasks[0].ledger)
	assert.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, b.schemaRewriteTasks[0].targetType)
	assert.Equal(t, "status", b.schemaRewriteTasks[0].key)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, b.schemaRewriteTasks[0].toType)

	expectedBBKey := schemaRewriteBBKey("test-ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	assert.Equal(t, expectedBBKey, b.schemaRewriteTasks[0].bbKey)
}

func TestAddSchemaRewriteTask_DuplicateResetsProgress(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	b.schemaRewriteTasks[0].rmapCursor = []byte("some-cursor")
	b.schemaRewriteTasks[0].processedCount = 100

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, b.schemaRewriteTasks[0].toType)
	assert.Nil(t, b.schemaRewriteTasks[0].rmapCursor)
	assert.Equal(t, uint64(0), b.schemaRewriteTasks[0].processedCount)
}

// TestAddSchemaRewriteTask_DuplicateClearsDoneProposed pins that a retype
// landing on a task whose previous rewrite already finished (done) or was
// proposed for IndexReady (proposed) restarts the lifecycle. Without the
// reset, tryProposeSchemaRewriteIndexReady would mark the index ready while
// rmap entries still encode the prior type.
func TestAddSchemaRewriteTask_DuplicateClearsDoneProposed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	b.schemaRewriteTasks[0].done = true
	b.schemaRewriteTasks[0].proposed = true
	b.schemaRewriteTasks[0].processedCount = 1234
	b.schemaRewriteTasks[0].requiredIndexedSeq = 42

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, b.schemaRewriteTasks[0].toType)
	assert.False(t, b.schemaRewriteTasks[0].done, "done must clear so the rewrite re-runs")
	assert.False(t, b.schemaRewriteTasks[0].proposed, "proposed must clear so IndexReady waits for the new rewrite")
	assert.Nil(t, b.schemaRewriteTasks[0].rmapCursor)
	assert.Equal(t, uint64(0), b.schemaRewriteTasks[0].processedCount)
	assert.Equal(t, uint64(0), b.schemaRewriteTasks[0].requiredIndexedSeq,
		"requiredIndexedSeq must clear so the new rewrite captures its own high-water mark")
}

func TestAddSchemaRewriteTask_Transaction(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tag")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
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

	bbKey1 := schemaRewriteBBKey("test-ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key1")
	bbKey2 := schemaRewriteBBKey("test-ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key2")
	bbKey3 := schemaRewriteBBKey("test-ledger", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key3")

	b.schemaRewriteTasks = []*schemaRewriteTask{
		{ledger: "ledger1", key: "key1", bbKey: bbKey1},
		{ledger: "ledger1", key: "key2", bbKey: bbKey2},
		{ledger: "ledger1", key: "key3", bbKey: bbKey3},
	}

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

// TestAddSchemaRewriteTask_ResetsInFlightBackfill pins the race window
// where SetMetadataFieldType arrives while an initial backfill is still
// running for the same metadata index. Scheduling a separate
// schemaRewriteTask would scan only the partial rmap and could propose
// IndexReady before the backfill replays the historical rows. The
// expected fix: reset the existing backfill's cursor to 0 (so it replays
// under the new declared_type), and do NOT enqueue a separate schema
// rewrite task.
func TestAddSchemaRewriteTask_ResetsInFlightBackfill(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	// Backfill already in flight: persisted cursor at seq 50, proposed=false.
	bbKey := backfillBBKey("ledger1", id)
	require.NoError(t, store.WriteBackfillProgress(store.NewBatch(), bbKey, 50))

	b.backfillTasks = []*backfillTask{
		{
			ledger:             "ledger1",
			index:              id,
			cursor:             50,
			appliedProposalSeq: 7,
			bbKey:              bbKey,
			proposed:           false,
		},
	}

	b.addSchemaRewriteTask(cfg, "ledger1", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "score",
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
	})

	assert.Empty(t, b.schemaRewriteTasks,
		"must NOT enqueue a schema rewrite while the initial backfill is still active")
	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, uint64(0), b.backfillTasks[0].cursor,
		"existing backfill must restart from 0 so it replays under the new declared_type")
	assert.Equal(t, uint64(0), b.backfillTasks[0].appliedProposalSeq, "audit cursor must reset too")
	assert.False(t, b.backfillTasks[0].proposed)
}

// TestAddSchemaRewriteTask_ResetsBackfillEvenWhenIndexStripped pins the
// crash-recovery scenario: the FSM applied SetMetadataFieldType, the node
// crashed before processLogs saw the retype, and on restart initIndexConfig
// reloaded the persisted backfill cursor. During the catch-up,
// stripBuildingIndexes temporarily removes the BUILDING index from cfg.
// When processLogs eventually reaches the retype log, addSchemaRewriteTask
// must still reset the backfill (regardless of cfg.isMetadataIndexed) so
// the replay restarts from 0 and emits forward entries under the new
// declared_type — otherwise the resumed backfill produces a mix of
// old/new typed entries and the index is marked READY incoherent.
func TestAddSchemaRewriteTask_ResetsBackfillEvenWhenIndexStripped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")

	// cfg is empty — simulates the state right after stripBuildingIndexes.
	cfg := newLedgerIndexConfig()
	b.indexConfig["ledger1"] = cfg

	bbKey := backfillBBKey("ledger1", id)
	require.NoError(t, store.WriteBackfillProgress(store.NewBatch(), bbKey, 250))

	b.backfillTasks = []*backfillTask{
		{
			ledger:             "ledger1",
			index:              id,
			cursor:             250,
			appliedProposalSeq: 9,
			bbKey:              bbKey,
		},
	}

	b.addSchemaRewriteTask(cfg, "ledger1", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "score",
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
	})

	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, uint64(0), b.backfillTasks[0].cursor,
		"stripped index must not block the retype reset — backfill must restart from 0")
	assert.Equal(t, uint64(0), b.backfillTasks[0].appliedProposalSeq)
	assert.Empty(t, b.schemaRewriteTasks, "no separate schema rewrite needed; backfill replay covers it")
}

// TestAddSchemaRewriteTask_DoesNotResetOtherLedgersBackfill guards against
// matching a backfillTask by (target, key) alone: a different ledger that
// happens to have the same indexed metadata key must NOT see its backfill
// reset, and the retyped ledger must still get its schemaRewriteTask
// enqueued.
func TestAddSchemaRewriteTask_DoesNotResetOtherLedgersBackfill(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	otherBBKey := backfillBBKey("other-ledger", id)
	b.backfillTasks = []*backfillTask{
		{
			ledger:             "other-ledger",
			index:              id,
			cursor:             42,
			appliedProposalSeq: 3,
			bbKey:              otherBBKey,
		},
	}

	b.addSchemaRewriteTask(cfg, "retyped-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "score",
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
	})

	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, uint64(42), b.backfillTasks[0].cursor,
		"the other ledger's backfill cursor must NOT be touched")
	assert.Equal(t, uint64(3), b.backfillTasks[0].appliedProposalSeq)

	require.Len(t, b.schemaRewriteTasks, 1, "the retyped ledger must still get its schema rewrite")
	assert.Equal(t, "retyped-ledger", b.schemaRewriteTasks[0].ledger)
}

// TestRemoveSchemaRewriteTaskByField pins that the helper cancels the right
// (ledger, target, key) task and leaves siblings intact. handleRemovedMetadataFieldType
// relies on this to avoid leaking a rewrite task once its underlying index
// is dropped — otherwise the builder would retry IndexReady proposals
// forever against a now-missing index.
func TestRemoveSchemaRewriteTaskByField(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
	}

	bbA := schemaRewriteBBKey("ledger1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	bbB := schemaRewriteBBKey("ledger1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	bbC := schemaRewriteBBKey("ledger1", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "score")
	bbD := schemaRewriteBBKey("ledger2", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")

	b.schemaRewriteTasks = []*schemaRewriteTask{
		{ledger: "ledger1", targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, key: "score", bbKey: bbA},
		{ledger: "ledger1", targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, key: "tier", bbKey: bbB},
		{ledger: "ledger1", targetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION, key: "score", bbKey: bbC},
		{ledger: "ledger2", targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, key: "score", bbKey: bbD},
	}

	b.removeSchemaRewriteTaskByField("ledger1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")

	require.Len(t, b.schemaRewriteTasks, 3)

	got := map[string]bool{}
	for _, task := range b.schemaRewriteTasks {
		got[task.ledger+"/"+task.targetType.String()+"/"+task.key] = true
	}

	assert.False(t, got["ledger1/TARGET_TYPE_ACCOUNT/score"], "matching task must be cancelled")
	assert.True(t, got["ledger1/TARGET_TYPE_ACCOUNT/tier"], "different key on same ledger/target must survive")
	assert.True(t, got["ledger1/TARGET_TYPE_TRANSACTION/score"], "different target must survive")
	assert.True(t, got["ledger2/TARGET_TYPE_ACCOUNT/score"], "different ledger must survive")

	// No-op when no task matches — must not panic.
	b.removeSchemaRewriteTaskByField("ledger1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
	require.Len(t, b.schemaRewriteTasks, 3)
}

// newTestBuilderWithStore creates a Builder backed by a temporary Pebble read store
// and a separate FSM Pebble store + attributes registry. The FSM side is needed
// because schema rewrites source canonical stored values from the FSM zone.
func newTestBuilderWithStore(t *testing.T) *Builder {
	t.Helper()

	rsDir := t.TempDir()
	store, err := readstore.New(rsDir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	fsmDir := t.TempDir()
	fsm, err := dal.NewStore(fsmDir, noopLogger{}, metricnoop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = fsm.Close() })

	return &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
		pebbleStore: fsm,
		attrs:       attributes.New(),
		logger:      noopLogger{},
	}
}

// seedBatchSchema primes b.batchSchema with a resolver backed by the
// Builder's FSM pebble store. Tests that invoke indexer write helpers
// (indexSavedMetadata, indexCreatedTransaction, indexRevertedTransaction)
// directly without going through processLogs / processBackfill must call
// this — b.coerceForLedger panics otherwise.
func (b *Builder) seedBatchSchema(t *testing.T) {
	t.Helper()

	handle, err := b.pebbleStore.NewDirectReadHandle()
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Close() })

	b.batchSchema = newSchemaResolver(handle, b.attrs)
}

// TestPersistedSchemaRewriteCursorTriggersBackfillRecovery pins the
// post-crash boot contract: a persisted schema-rewrite cursor means the
// previous shutdown left a rewrite unfinished. Since cluster-wide
// IndexReady is unilaterally proposed by the first node to finish (not a
// consensus signal), LedgerInfo may already say READY by reboot — the
// BUILDING flag alone misses the case. initIndexConfig must therefore
// scan persisted schema-rewrite cursors and schedule a full backfill
// (cursor 0) for each, regardless of LedgerInfo's BuildStatus. The
// stale schema-rewrite cursor itself is dropped because the backfill
// uses its own bbKey.
func TestPersistedSchemaRewriteCursorTriggersBackfillRecovery(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.logger = noopLogger{}

	const (
		ledger = "customer"
		key    = "role"
	)

	// Persist the unfinished schema-rewrite cursor.
	rewriteBBKey := schemaRewriteBBKey(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	val := append([]byte{byte(commonpb.MetadataType_METADATA_TYPE_STRING)}, []byte("mid-rewrite-cursor")...)

	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteBackfillCursor(batch, rewriteBBKey, val))
	require.NoError(t, batch.Commit())

	// Seed the index registry with the index already marked READY — exactly
	// the state we'd see if another node had finished and proposed
	// IndexReady before this node crashed.
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	ledgerCanonicalKey := domain.LedgerKey{Name: ledger}.Bytes()
	indexCanonicalKey := domain.IndexKey{LedgerName: ledger, Canonical: indexes.Canonical(id)}.Bytes()
	fsmBatch := b.pebbleStore.OpenWriteSession()
	_, err := b.attrs.Ledger.Set(fsmBatch, ledgerCanonicalKey, &commonpb.LedgerInfo{
		Name: ledger,
	})
	require.NoError(t, err)
	_, err = b.attrs.Index.Set(fsmBatch, indexCanonicalKey, &commonpb.Index{
		Ledger:      ledger,
		Id:          id,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	b.initIndexConfig(context.Background())

	// Recovery scheduled a backfill for the index even though it's READY.
	require.Len(t, b.backfillTasks, 1, "the persisted schema-rewrite cursor must trigger a backfill")
	assert.Equal(t, ledger, b.backfillTasks[0].ledger)
	assert.True(t, indexes.Equal(b.backfillTasks[0].index, id))
	assert.Equal(t, uint64(0), b.backfillTasks[0].cursor, "backfill must restart from 0")
	assert.False(t, b.backfillTasks[0].proposed)

	// The stale schema-rewrite cursor has been cleaned up.
	_, ok := b.readStore.ReadBackfillCursor(rewriteBBKey)
	assert.False(t, ok, "stale schema-rewrite cursor must be deleted after recovery")
}

// noopLogger implements the logging.Logger interface for tests without output.
// noopLogger stays hand-rolled: logging.Logger lives in
// github.com/formancehq/go-libs and has no mockgen directive upstream. A
// silent fixture is simpler than a vendor mock here.
type noopLogger struct{}

var _ logging.Logger = noopLogger{}

func (noopLogger) Tracef(string, ...any)                        {}
func (noopLogger) Debugf(string, ...any)                        {}
func (noopLogger) Infof(string, ...any)                         {}
func (noopLogger) Errorf(string, ...any)                        {}
func (noopLogger) Trace(...any)                                 {}
func (noopLogger) Debug(...any)                                 {}
func (noopLogger) Info(...any)                                  {}
func (noopLogger) Error(...any)                                 {}
func (n noopLogger) WithFields(map[string]any) logging.Logger   { return n }
func (n noopLogger) WithField(string, any) logging.Logger       { return n }
func (n noopLogger) WithContext(context.Context) logging.Logger { return n }
func (noopLogger) Writer() io.Writer                            { return io.Discard }
func (noopLogger) Enabled(logging.Level) bool                   { return false }
