package indexbuilder

import (
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// paddedLedgerName returns the ledger name zero-padded to LedgerNameFixedSize
// — matching the canonical encoding used by backfill/schemaRewrite BB keys.
func paddedLedgerName(name string) []byte {
	out := make([]byte, dal.LedgerNameFixedSize)
	copy(out, name)

	return out
}

func TestSchemaRewriteBBKey_Account(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("test", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))
	expected = append(expected, "status"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_Transaction(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("test", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_TRANSACTION))
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_EmptyMetadataKey(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey("test", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "")

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_DifferentTargetTypesProduceDifferentKeys(t *testing.T) {
	t.Parallel()

	keyAcct := schemaRewriteBBKey("test", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key")
	keyTx := schemaRewriteBBKey("test", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "key")

	assert.NotEqual(t, keyAcct, keyTx)
}

func TestBackfillBBKey_TxBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("test", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindTxBuiltin, byte(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_TxMetadata(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("test", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"))

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindTxMetadata)
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("test", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindAcctBuiltin, byte(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctMetadata(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("test", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"))

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindAcctMetadata)
	expected = append(expected, "role"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_LogBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey("test", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))

	expected := paddedLedgerName("test")
	expected = append(expected, readstore.BackfillKindLogBuiltin, byte(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_NilID_ReturnsNil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, backfillBBKey("test", nil))
	assert.Nil(t, backfillBBKey("test", &commonpb.IndexID{}))
}

func TestBackfillIndexName_TxBuiltin(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
	assert.Equal(t, "tx:"+commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE.String(), name)
}

func TestBackfillIndexName_TxMetadata(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"))
	assert.Equal(t, "tx:metadata:category", name)
}

func TestBackfillIndexName_AcctBuiltin(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))
	assert.Equal(t, "acct:"+commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED.String(), name)
}

func TestBackfillIndexName_AcctMetadata(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"))
	assert.Equal(t, "acct:metadata:role", name)
}

func TestBackfillIndexName_LogBuiltin(t *testing.T) {
	t.Parallel()

	name := backfillIndexName(indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))
	assert.Equal(t, "log:"+commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE.String(), name)
}

func TestBackfillIndexName_Unknown(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "unknown", backfillIndexName(nil))
	assert.Equal(t, "unknown", backfillIndexName(&commonpb.IndexID{}))
}

func TestBuildBackfillConfig_TxBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	task := &backfillTask{ledger: "test", index: id}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.isIndexed(id))
	assert.False(t, cfg.isIndexed(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)))
}

func TestBuildBackfillConfig_AcctMetadata(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	task := &backfillTask{ledger: "test", index: id}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.isIndexed(id))
	assert.False(t, cfg.isIndexed(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "other")))
}

func TestBuildBackfillConfig_LogBuiltin(t *testing.T) {
	t.Parallel()

	b := &Builder{indexConfig: make(map[string]*ledgerIndexConfig)}
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	task := &backfillTask{ledger: "test", index: id}

	cfg := b.buildBackfillConfig(task)

	require.NotNil(t, cfg)
	assert.True(t, cfg.isIndexed(id))
}

func TestIndexLogEntryUsesReplayAuditSyncForExcludedAccounts(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
		accounts:  make(map[string]struct{}),
	}

	batch := store.NewBatch()
	b.wb.Init(batch)

	cfg := newLedgerIndexConfig()
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	audit := newTestAuditSync(
		testAuditEntry(1, 1, 10, "ledger-a", "stale:account"),
		testAuditEntry(2, 20, 20, "ledger-b", "transient:source"),
	)

	log := makeCreatedTxLog(20, "ledger-b", 99, []*commonpb.Posting{
		{Source: "transient:source", Destination: "kept:dest"},
	})

	require.NoError(t, b.indexLogEntry(cfg, log, audit))
	require.NoError(t, b.wb.Flush())

	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "ledger-b", "transient:source", 99,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "ledger-b", "kept:dest", 99,
	)))
}

func TestIndexPostingAddressMappingsSkipsExcludedAccounts(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		kb: dal.NewKeyBuilder(),
		wb: readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.wb.Init(batch)

	excludedAccounts := map[string]struct{}{
		"transient:source": {},
		"purged:dest":      {},
	}

	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, "test", 42, "transient:source", "kept:dest",
		true, true, true, excludedAccounts,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, "test", 43, "kept:source", "purged:dest",
		true, true, true, excludedAccounts,
	))
	require.NoError(t, b.wb.Flush())

	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "transient:source", 42,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, "test", "transient:source", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "kept:dest", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixDestAccountTx, "test", "kept:dest", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "kept:source", 43,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, "test", "kept:source", 43,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "purged:dest", 43,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixDestAccountTx, "test", "purged:dest", 43,
	)))
}

func TestAuditSyncResumeSequenceOnlySkipsFullyConsumedRanges(t *testing.T) {
	t.Parallel()

	audit := newTestAuditSync(
		testAuditEntry(1, 1, 10, "ledger", "old:account"),
		testAuditEntry(2, 20, 30, "ledger", "current:account"),
	)

	excluded := audit.syncTo(25, "ledger")
	require.Contains(t, excluded, "current:account")
	assert.Equal(t, uint64(1), audit.resumeSequence())

	audit.advanceBefore(31)
	assert.Equal(t, uint64(2), audit.resumeSequence())
}

func TestAuditSyncResumeSequenceKeepsInitialResumeFloor(t *testing.T) {
	t.Parallel()

	audit := newTestAuditSyncAfter(1,
		testAuditEntry(2, 20, 30, "ledger", "current:account"),
	)

	excluded := audit.syncTo(25, "ledger")
	require.Contains(t, excluded, "current:account")
	assert.Equal(t, uint64(1), audit.resumeSequence())
}

func newTestAuditSync(entries ...*auditpb.AuditEntry) *auditSync {
	return newTestAuditSyncAfter(0, entries...)
}

func newTestAuditSyncAfter(afterAuditSeq uint64, entries ...*auditpb.AuditEntry) *auditSync {
	audit := &auditSync{cursor: cursor.NewSliceCursor(entries)}
	audit.resumeAfterAuditSeq = afterAuditSeq
	audit.advance()

	return audit
}

func testAuditEntry(sequence, minLogSeq, maxLogSeq uint64, ledger string, accounts ...string) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence: sequence,
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: minLogSeq,
				MaxLogSequence: maxLogSeq,
				TransientAccounts: map[string]*auditpb.AccountList{
					ledger: {Accounts: accounts},
				},
			},
		},
	}
}

func readStoreKeyExists(t *testing.T, store *readstore.Store, key []byte) bool {
	t.Helper()

	_, closer, err := store.DB().Get(key)
	if err == nil {
		require.NoError(t, closer.Close())

		return true
	}

	if errors.Is(err, pebble.ErrNotFound) {
		return false
	}

	require.NoError(t, err)

	return false
}

// TestIndexSavedMetadata_OverwriteDeletesByReverseMapDuringBuilding guards the
// fix for an index desync during the BUILDING window. The incremental update
// deletes the old entry using the index's own reverse-map value, not the log's
// previous value: while the schema-rewrite backfill has not yet rewritten an
// entity's entry, the index still holds the pre-conversion (raw) encoding, which
// differs from the coerced previous value in the log. Here the log carries the
// coerced Int64(30) but the index entry is the raw String "30"; the delete must
// still hit, leaving no stale entry.
func TestIndexSavedMetadata_OverwriteDeletesByReverseMapDuringBuilding(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.kb = dal.NewKeyBuilder()
	b.wb = readstore.NewWriteBatch()

	kb := b.kb
	const (
		ledger  = "test"
		account = "acct-001"
		key     = "age"
	)
	entityID := []byte(account)

	// Pre-conversion index state: age was written as a string before the INT64
	// type was declared, and the schema-rewrite backfill has not yet rewritten
	// this entity's entry — so both the reverse map and the forward index hold
	// the raw string encoding.
	rawEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("30"))
	reverseKey := cloneBytes(readstore.AccountReverseMapKey(kb, ledger, account, key))
	rawForwardKey := cloneBytes(readstore.MetadataIndexKey(kb, ledger, readstore.NamespaceAccount, key, rawEncoded, entityID))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(reverseKey, rawEncoded))
	require.NoError(t, seed.SetBytes(rawForwardKey, nil))
	require.NoError(t, seed.Commit())

	// The "age" account index is BUILDING; incremental writes still flow to it.
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{
		Id:          id,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
	}

	// Incremental write age="40". The FSM coerces both the new value and the
	// captured previous value to the declared INT64 type.
	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Metadata:       map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(40)},
		PreviousValues: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(30)},
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	require.NoError(t, b.indexSavedMetadata(kb, cfg, ledger, sm))
	require.NoError(t, b.wb.Flush())

	// New entry exists and the reverse map points at it.
	newForwardKey := readstore.MetadataIndexKey(kb, ledger, readstore.NamespaceAccount, key,
		readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(40)), entityID)
	assertReadStoreValue(t, b, newForwardKey, nil)
	assertReadStoreValue(t, b, reverseKey, readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(40)))

	// The pre-conversion entry is gone: the delete was located via the reverse
	// map (raw String "30"), not the log's coerced Int64(30), so a query for the
	// old value no longer returns acct-001.
	assertReadStoreMissing(t, b, rawForwardKey)
}

// TestIndexCreatedThenOverwrittenTxMetadataSameBatch guards the overlay against
// the create-then-overwrite-in-one-batch interleaving: a transaction sets indexed
// metadata at creation (first write, no previous value), and a later SaveMetadata
// in the SAME uncommitted batch overwrites the same key. The create write is not
// yet committed, so the overwrite can only find the entry to delete via the
// overlay — the create path must mirror its reverse-map write there.
func TestIndexCreatedThenOverwrittenTxMetadataSameBatch(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.kb = dal.NewKeyBuilder()
	b.wb = readstore.NewWriteBatch()
	b.accounts = make(map[string]struct{})

	kb := b.kb
	const (
		ledger = "test"
		txID   = uint64(7)
		key    = "category"
	)
	txIDBytes := readstore.EncodeTxID(make([]byte, 0, 8), txID)

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	v1 := commonpb.NewStringValue("v1")
	v2 := commonpb.NewStringValue("v2")
	// Clone: KeyBuilder returns buffer-backed slices that later index calls reuse.
	v1Key := cloneBytes(readstore.MetadataIndexKey(kb, ledger, readstore.NamespaceTransaction, key,
		readstore.EncodeMetadataValue(nil, v1), txIDBytes))
	v2Key := cloneBytes(readstore.MetadataIndexKey(kb, ledger, readstore.NamespaceTransaction, key,
		readstore.EncodeMetadataValue(nil, v2), txIDBytes))
	reverseKey := cloneBytes(readstore.TransactionReverseMapKey(kb, ledger, txID, key))

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)

	// 1. CreateTransaction sets indexed tx metadata key=v1 (first write).
	ct := &commonpb.CreatedTransaction{
		Transaction: &commonpb.Transaction{
			Id:       txID,
			Metadata: map[string]*commonpb.MetadataValue{key: v1},
		},
	}
	require.NoError(t, b.indexCreatedTransaction(kb, cfg, ledger, ct, nil))

	// 2. Same batch: overwrite the same key to v2 before the batch commits.
	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_TransactionId{TransactionId: txID},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: v2},
	}
	require.NoError(t, b.indexSavedMetadata(kb, cfg, ledger, sm))

	require.NoError(t, b.wb.Flush())

	// v2 is indexed and the stale v1 entry from the create is gone.
	assertReadStoreValue(t, b, v2Key, nil)
	assertReadStoreValue(t, b, reverseKey, readstore.EncodeMetadataValue(nil, v2))
	assertReadStoreMissing(t, b, v1Key)
}

func TestProcessSchemaRewriteCountsScannedKeysAgainstBudgetAndPersistsCursor(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})
	ledgerName := "test"

	firstSkippedKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-001", "other")
	secondSkippedKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-002", "other")
	matchingKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-003", "status")

	skippedEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("ignored"))
	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("42"))
	newEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))
	entityID := []byte("acct-003")
	oldForwardKey := cloneBytes(readstore.MetadataIndexKey(kb, ledgerName, readstore.NamespaceAccount, "status", oldEncoded, entityID))
	newForwardKey := cloneBytes(readstore.MetadataIndexKey(kb, ledgerName, readstore.NamespaceAccount, "status", newEncoded, entityID))

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(firstSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(secondSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(matchingKey, oldEncoded))
	require.NoError(t, batch.SetBytes(oldForwardKey, nil))
	require.NoError(t, batch.Commit())

	task := &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        "status",
		toType:     commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:      schemaRewriteBBKey(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
	}

	done, err := b.processSchemaRewrite(task, 2, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, uint64(0), task.processedCount)
	assert.Equal(t, secondSkippedKey, task.rmapCursor)

	cursor, ok := b.readStore.ReadBackfillCursor(task.bbKey)
	require.True(t, ok)
	assert.Equal(t, append([]byte{byte(task.toType)}, secondSkippedKey...), cursor)
	assertReadStoreValue(t, b, oldForwardKey, nil)
	assertReadStoreValue(t, b, matchingKey, oldEncoded)

	done, err = b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, uint64(1), task.processedCount)
	assert.Equal(t, matchingKey, task.rmapCursor)

	assertReadStoreMissing(t, b, oldForwardKey)
	assertReadStoreValue(t, b, newForwardKey, nil)
	assertReadStoreValue(t, b, matchingKey, newEncoded)
}

func TestProcessSchemaRewriteStopsBeforeScanningWhenStopClosed(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})
	close(stop)

	ledgerName := "test"
	reverseKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-001", "status")
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("42"))

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(reverseKey, encoded))
	require.NoError(t, batch.Commit())

	task := &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        "status",
		toType:     commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:      schemaRewriteBBKey(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
	}

	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, done)
	assert.Equal(t, uint64(0), task.processedCount)
	assert.Nil(t, task.rmapCursor)

	_, ok := b.readStore.ReadBackfillCursor(task.bbKey)
	assert.False(t, ok)
	assertReadStoreValue(t, b, reverseKey, encoded)
}

func assertReadStoreValue(t *testing.T, b *Builder, key, expected []byte) {
	t.Helper()

	value, closer, err := b.readStore.DB().Get(key)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()

	assert.Equal(t, expected, append([]byte(nil), value...))
}

func assertReadStoreMissing(t *testing.T, b *Builder, key []byte) {
	t.Helper()

	_, closer, err := b.readStore.DB().Get(key)
	if closer != nil {
		defer func() { require.NoError(t, closer.Close()) }()
	}

	require.True(t, errors.Is(err, pebble.ErrNotFound), "expected key %x to be missing, got %v", key, err)
}

func TestIsDataLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		log      *commonpb.Log
		expected bool
	}{
		{
			name:     "nil payload",
			log:      &commonpb.Log{},
			expected: false,
		},
		{
			name: "non-apply payload",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{},
				},
			},
			expected: false,
		},
		{
			name: "apply with nil log",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{},
					},
				},
			},
			expected: false,
		},
		{
			name: "apply with nil data",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "created transaction",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreatedTransaction{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "reverted transaction",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_RevertedTransaction{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "saved metadata",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_SavedMetadata{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "deleted metadata",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_DeletedMetadata{},
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "create index (config mutation)",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreateIndex{},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "drop index (config mutation)",
			log: &commonpb.Log{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_DropIndex{},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.expected, isDataLog(tc.log))
		})
	}
}

func TestIsPostingIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       *commonpb.IndexID
		expected bool
	}{
		{name: "nil", id: nil, expected: false},
		{
			name:     "metadata key (not posting)",
			id:       indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"),
			expected: false,
		},
		{
			name:     "address index",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
			expected: true,
		},
		{
			name:     "source address index",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS),
			expected: true,
		},
		{
			name:     "dest address index",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS),
			expected: true,
		},
		{
			name:     "reference index (not posting)",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			expected: false,
		},
		{
			name:     "timestamp index (not posting)",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
			expected: false,
		},
		{
			name:     "account metadata (not posting)",
			id:       indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, isPostingIndex(tc.id))
		})
	}
}
