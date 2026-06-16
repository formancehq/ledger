package indexbuilder

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

	assert.Nil(t, b.ledgerConfig("unknown"))

	cfg := newLedgerIndexConfig()
	b.indexConfig["test"] = cfg
	assert.Same(t, cfg, b.ledgerConfig("test"))
}

func TestGetOrCreateLedgerConfig(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

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

			b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

			b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{Id: tt.id})

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
	b.handleCreatedIndexLog("ledger1", 1, &commonpb.CreatedIndexLog{Id: id})
	require.Len(t, b.backfillTasks, 1)
	assert.True(t, b.indexConfig["ledger1"].isIndexed(id))

	b.handleDroppedIndexLog("ledger1", &commonpb.DroppedIndexLog{Id: id})
	assert.False(t, b.indexConfig["ledger1"].isIndexed(id))
	assert.Empty(t, b.backfillTasks)
}

func TestAddBackfillTask_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

	assert.Len(t, b.backfillTasks, 1)
}

func TestAddBackfillTask_DifferentIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	b.addBackfillTaskForAcctMetadata("ledger1", 1, "role")
	b.addBackfillTaskForLogBuiltin("ledger1", 1, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	assert.Len(t, b.backfillTasks, 5)
}

func TestStripBuildingIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

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
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	b.addBackfillTaskForAcctMetadata("ledger1", 1, "role")
	b.addBackfillTaskForLogBuiltin("ledger1", 1, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

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

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: "missing",
		index:  id,
		bbKey:  backfillBBKey(99, id),
	})

	restore := b.stripBuildingIndexes()
	restore()
}

func TestLoadLedgerIndexConfig_FromIndexes(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}

	refID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	tsID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	roleID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	categoryID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")
	dateID := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)

	info := &commonpb.LedgerInfo{
		Name: "ledger1",
		Indexes: []*commonpb.Index{
			{Id: refID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
			{Id: tsID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY},
			{Id: roleID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
			{Id: categoryID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
			{Id: dateID, BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING},
		},
	}

	b.loadLedgerIndexConfig(info)

	cfg := b.indexConfig["ledger1"]
	require.NotNil(t, cfg)
	assert.True(t, cfg.isIndexed(refID))
	assert.True(t, cfg.isIndexed(tsID))
	assert.True(t, cfg.isIndexed(roleID))
	assert.True(t, cfg.isIndexed(categoryID))
	assert.True(t, cfg.isIndexed(dateID))

	// Only BUILDING entries trigger backfill scheduling (4 of the 5 above).
	assert.Len(t, b.backfillTasks, 4)
}

func TestRemoveBackfillTask(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	b.addBackfillTaskForTxBuiltin("ledger1", 1, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)
	b.addBackfillTaskForTxMetadata("ledger1", 1, "category")
	require.Len(t, b.backfillTasks, 3)

	b.removeBackfillTask(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP))
	assert.Len(t, b.backfillTasks, 2)

	// Removing one that doesn't exist is a no-op.
	b.removeBackfillTask(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT))
	assert.Len(t, b.backfillTasks, 2)
}

func TestAddSchemaRewriteTask_NotIndexed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}
	cfg := newLedgerIndexConfig()

	b.addSchemaRewriteTask(cfg, 1, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	assert.Empty(t, b.schemaRewriteTasks)
}

func TestAddSchemaRewriteTask_Indexed(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, 1, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
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

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, 1, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "status",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	})

	require.Len(t, b.schemaRewriteTasks, 1)
	b.schemaRewriteTasks[0].rmapCursor = []byte("some-cursor")
	b.schemaRewriteTasks[0].processedCount = 100

	b.addSchemaRewriteTask(cfg, 1, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
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

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig), ledgerNameToID: make(map[string]uint32)}
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tag")
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	b.addSchemaRewriteTask(cfg, 1, "test-ledger", &commonpb.SetMetadataFieldTypeLog{
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

// newTestBuilderWithStore creates a Builder backed by a temporary Pebble read store.
func newTestBuilderWithStore(t *testing.T) *Builder {
	t.Helper()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	return &Builder{
		indexConfig:    make(map[string]*ledgerIndexConfig),
		ledgerNameToID: make(map[string]uint32),
		readStore:      store,
	}
}

// TestRecoverSchemaRewriteTasks_ResolvesLedgerNameFromID is the
// regression for PR #277. A persisted schema rewrite task only stores
// the ledgerID; on restart the recovery loop MUST resolve that ID back
// to the ledger name so proposeSchemaRewriteIndexReady can address
// LedgerInfo by name. If the name were left empty, the FSM check
// query.GetLedgerByName("") would never return the ledger and the
// index would stay BUILDING forever.
func TestRecoverSchemaRewriteTasks_ResolvesLedgerNameFromID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	// Persist a schema rewrite progress entry for ledgerID=42 ("customer"),
	// account-metadata key "role", target type STRING, with a non-empty
	// resume cursor. Value format mirrors backfill.go: [toType_byte][cursor].
	const (
		ledgerID   = uint32(42)
		ledgerName = "customer"
		metaKey    = "role"
		targetType = commonpb.TargetType_TARGET_TYPE_ACCOUNT
		toType     = commonpb.MetadataType_METADATA_TYPE_STRING
	)

	cursor := []byte("rmap-cursor-bytes")
	bbKey := schemaRewriteBBKey(ledgerID, targetType, metaKey)
	val := append([]byte{byte(toType)}, cursor...)

	batch := store.NewBatch()
	require.NoError(t, store.WriteBackfillCursor(batch, bbKey, val))
	require.NoError(t, batch.Commit())

	b := &Builder{
		indexConfig:    make(map[string]*ledgerIndexConfig),
		ledgerNameToID: map[string]uint32{ledgerName: ledgerID},
		readStore:      store,
		logger:         noopLogger{},
	}

	b.recoverSchemaRewriteTasks()

	require.Len(t, b.schemaRewriteTasks, 1)
	task := b.schemaRewriteTasks[0]
	assert.Equal(t, ledgerName, task.ledger,
		"ledger name MUST be resolved from ledgerID — empty name makes IndexReadyUpdate unaddressable (#277)")
	assert.Equal(t, ledgerID, task.ledgerID)
	assert.Equal(t, targetType, task.targetType)
	assert.Equal(t, metaKey, task.key)
	assert.Equal(t, toType, task.toType)
	assert.Equal(t, cursor, task.rmapCursor)
	assert.Equal(t, bbKey, task.bbKey)
}

// TestRecoverSchemaRewriteTasks_DropsOrphanedEntries verifies that a
// persisted task whose ledger no longer exists (deleted between persist
// and restart) is dropped rather than re-enqueued forever with a stale
// name. Keeping it would burn a slot in the round-robin processor.
func TestRecoverSchemaRewriteTasks_DropsOrphanedEntries(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	store, err := readstore.New(dir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	bbKey := schemaRewriteBBKey(99, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	val := []byte{byte(commonpb.MetadataType_METADATA_TYPE_STRING)}

	batch := store.NewBatch()
	require.NoError(t, store.WriteBackfillCursor(batch, bbKey, val))
	require.NoError(t, batch.Commit())

	// ledgerNameToID is empty — the ledger that owned this task is gone.
	b := &Builder{
		indexConfig:    make(map[string]*ledgerIndexConfig),
		ledgerNameToID: map[string]uint32{},
		readStore:      store,
		logger:         noopLogger{},
	}

	b.recoverSchemaRewriteTasks()

	assert.Empty(t, b.schemaRewriteTasks,
		"orphaned schema rewrite task must be dropped, not retained with empty ledger name")
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
