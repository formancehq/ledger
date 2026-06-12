package indexbuilder

import (
	"encoding/binary"
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

func uint32BE(v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)

	return buf[:]
}

func TestSchemaRewriteBBKey_Account(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")

	expected := uint32BE(1)
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))
	expected = append(expected, "status"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_Transaction(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey(2, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category")

	expected := uint32BE(2)
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_TRANSACTION))
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_EmptyMetadataKey(t *testing.T) {
	t.Parallel()

	key := schemaRewriteBBKey(3, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "")

	expected := uint32BE(3)
	expected = append(expected, readstore.BackfillKindSchemaRewrite, byte(commonpb.TargetType_TARGET_TYPE_ACCOUNT))

	assert.Equal(t, expected, key)
}

func TestSchemaRewriteBBKey_DifferentTargetTypesProduceDifferentKeys(t *testing.T) {
	t.Parallel()

	keyAcct := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "key")
	keyTx := schemaRewriteBBKey(1, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "key")

	assert.NotEqual(t, keyAcct, keyTx)
}

func TestBackfillBBKey_TxBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey(1, indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))

	expected := uint32BE(1)
	expected = append(expected, readstore.BackfillKindTxBuiltin, byte(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_TxMetadata(t *testing.T) {
	t.Parallel()

	key := backfillBBKey(2, indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "category"))

	expected := uint32BE(2)
	expected = append(expected, readstore.BackfillKindTxMetadata)
	expected = append(expected, "category"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey(1, indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))

	expected := uint32BE(1)
	expected = append(expected, readstore.BackfillKindAcctBuiltin, byte(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_AcctMetadata(t *testing.T) {
	t.Parallel()

	key := backfillBBKey(1, indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"))

	expected := uint32BE(1)
	expected = append(expected, readstore.BackfillKindAcctMetadata)
	expected = append(expected, "role"...)

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_LogBuiltin(t *testing.T) {
	t.Parallel()

	key := backfillBBKey(1, indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))

	expected := uint32BE(1)
	expected = append(expected, readstore.BackfillKindLogBuiltin, byte(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE))

	assert.Equal(t, expected, key)
}

func TestBackfillBBKey_NilID_ReturnsNil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, backfillBBKey(1, nil))
	assert.Nil(t, backfillBBKey(1, &commonpb.IndexID{}))
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
		readStore:      store,
		kb:             dal.NewKeyBuilder(),
		wb:             readstore.NewWriteBatch(),
		accounts:       make(map[string]struct{}),
		ledgerNameToID: map[string]uint32{"ledger-a": 1, "ledger-b": 2},
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
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 2, "transient:source", 99,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 2, "kept:dest", 99,
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
		b.kb, 7, 42, "transient:source", "kept:dest",
		true, true, true, excludedAccounts,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, 7, 43, "kept:source", "purged:dest",
		true, true, true, excludedAccounts,
	))
	require.NoError(t, b.wb.Flush())

	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 7, "transient:source", 42,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, 7, "transient:source", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 7, "kept:dest", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixDestAccountTx, 7, "kept:dest", 42,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 7, "kept:source", 43,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, 7, "kept:source", 43,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, 7, "purged:dest", 43,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixDestAccountTx, 7, "purged:dest", 43,
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

func TestProcessSchemaRewriteCountsScannedKeysAgainstBudgetAndPersistsCursor(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})
	ledgerID := uint32(1)

	firstSkippedKey := readstore.AccountReverseMapKey(kb, ledgerID, "acct-001", "other")
	secondSkippedKey := readstore.AccountReverseMapKey(kb, ledgerID, "acct-002", "other")
	matchingKey := readstore.AccountReverseMapKey(kb, ledgerID, "acct-003", "status")

	skippedEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("ignored"))
	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("42"))
	newEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))
	entityID := []byte("acct-003")
	oldForwardKey := cloneBytes(readstore.MetadataIndexKey(kb, ledgerID, readstore.NamespaceAccount, "status", oldEncoded, entityID))
	newForwardKey := cloneBytes(readstore.MetadataIndexKey(kb, ledgerID, readstore.NamespaceAccount, "status", newEncoded, entityID))

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(firstSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(secondSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(matchingKey, oldEncoded))
	require.NoError(t, batch.SetBytes(oldForwardKey, nil))
	require.NoError(t, batch.Commit())

	task := &schemaRewriteTask{
		ledgerID:   ledgerID,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        "status",
		toType:     commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:      schemaRewriteBBKey(ledgerID, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
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

	ledgerID := uint32(1)
	reverseKey := readstore.AccountReverseMapKey(kb, ledgerID, "acct-001", "status")
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("42"))

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(reverseKey, encoded))
	require.NoError(t, batch.Commit())

	task := &schemaRewriteTask{
		ledgerID:   ledgerID,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        "status",
		toType:     commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:      schemaRewriteBBKey(ledgerID, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
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
