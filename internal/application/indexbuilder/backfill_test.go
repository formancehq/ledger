package indexbuilder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/query"
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

	proposals := newTestAppliedProposalSync(
		testAppliedProposal(1, 1, 10, "ledger-a", "stale:account"),
		testAppliedProposal(2, 20, 20, "ledger-b", "transient:source"),
	)

	log := makeCreatedTxLog(20, "ledger-b", 99, []*commonpb.Posting{
		{Source: "transient:source", Destination: "kept:dest", Asset: "USD"},
	})

	require.NoError(t, b.indexLogEntry(cfg, log, proposals))
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

	// Account-by-asset index not registered: the posting walk exercises only
	// the address mappings under test.
	cfg := newLedgerIndexConfig()

	// Multi-asset case: account "shared:account" has USD purged but EUR
	// kept. transient:source / purged:dest are excluded on USD only;
	// shared:account on USD is excluded but shared:account on EUR must be
	// indexed normally.
	excludedVolumes := map[domain.AccountAssetKey]struct{}{
		{Account: "transient:source", Asset: "USD"}: {},
		{Account: "purged:dest", Asset: "USD"}:      {},
		{Account: "shared:account", Asset: "USD"}:   {},
	}

	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 42, "transient:source", "kept:dest", "USD", "",
		true, true, true, excludedVolumes,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 43, "kept:source", "purged:dest", "USD", "",
		true, true, true, excludedVolumes,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 44, "shared:account", "kept:dest", "USD", "",
		true, true, true, excludedVolumes,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 45, "shared:account", "kept:dest", "EUR", "",
		true, true, true, excludedVolumes,
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
		dal.NewKeyBuilder(), readstore.PrefixDestinationAccountTx, "test", "kept:dest", 42,
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
		dal.NewKeyBuilder(), readstore.PrefixDestinationAccountTx, "test", "purged:dest", 43,
	)))

	// Multi-asset: shared:account USD (tx 44) is excluded, EUR (tx 45) is not.
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "shared:account", 44,
	)))
	assert.False(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, "test", "shared:account", 44,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixAccountTx, "test", "shared:account", 45,
	)))
	assert.True(t, readStoreKeyExists(t, store, readstore.AccountTxKey(
		dal.NewKeyBuilder(), readstore.PrefixSourceAccountTx, "test", "shared:account", 45,
	)))
}

// scanAccountByAsset returns the set of accounts recorded in the
// account-by-asset index for (ledger, assetBase, precision).
func scanAccountByAsset(t *testing.T, store *readstore.Store, ledger, assetBase string, precision uint8) map[string]struct{} {
	t.Helper()

	prefix := readstore.AccountByAssetPrefix(dal.NewKeyBuilder(), ledger, assetBase, precision)
	iter, err := store.DB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	out := make(map[string]struct{})
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		// Key layout: [prefix][account]; the account is the suffix after prefix.
		out[string(k[len(prefix):])] = struct{}{}
	}

	require.NoError(t, iter.Error())

	return out
}

// acctAssetConfig builds a ledgerIndexConfig with the account has-asset
// builtin index registered (plus the address builtin so the posting walk
// runs at all).
func acctAssetConfig() *ledgerIndexConfig {
	cfg := newLedgerIndexConfig()
	cfg.byCanonical[indexes.Canonical(indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET))] =
		&commonpb.Index{Id: indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)}

	return cfg
}

func TestIndexPostingAddressMappingsWritesAccountByAsset(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	cfg := acctAssetConfig()

	// source=accounts:alice destination=accounts:bob asset="USD/2".
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
		false, false, false, nil,
	))
	require.NoError(t, b.wb.Flush())

	got := scanAccountByAsset(t, store, "test", "USD", 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:alice": {},
		"accounts:bob":   {},
	}, got)
}

func TestIndexPostingAddressMappingsAccountByAssetDedup(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	cfg := acctAssetConfig()

	// Feed the same posting twice within the same batch.
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
		false, false, false, nil,
	))
	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 2, "accounts:alice", "accounts:bob", "USD/2", "",
		false, false, false, nil,
	))
	require.NoError(t, b.wb.Flush())

	got := scanAccountByAsset(t, store, "test", "USD", 2)
	assert.Len(t, got, 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:alice": {},
		"accounts:bob":   {},
	}, got)
}

// TestIndexPostingAddressMappingsAccountByAssetSurvivesInBatchDelete guards the
// dedup against a DeleteLedger that lands in the same indexer batch as renewed
// account-by-asset activity for a same-name (recreated) ledger. DeleteLedger
// only queues a by-name range delete (no batch flush), and readstoreKeyExists
// reads committed Pebble directly — so without the in-batch-delete guard the
// dedup sees the about-to-be-deleted row, skips the new Put, and the range
// delete then wipes the row, silently dropping the account from has-asset
// queries.
func TestIndexPostingAddressMappingsAccountByAssetSurvivesInBatchDelete(t *testing.T) {
	t.Parallel()

	t.Run("committed row pending range-delete does not suppress the recreate", func(t *testing.T) {
		t.Parallel()

		store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
		require.NoError(t, err)

		defer func() { _ = store.Close() }()

		b := &Builder{
			readStore: store,
			kb:        dal.NewKeyBuilder(),
			wb:        readstore.NewWriteBatch(),
		}
		cfg := acctAssetConfig()

		// Batch 1: commit account-by-asset rows for the original ledger.
		batch1 := store.NewBatch()
		b.initBatch(batch1)
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexPostingAddressMappings(
			b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
			false, false, false, nil,
		))
		require.NoError(t, b.wb.Flush())
		require.Len(t, scanAccountByAsset(t, store, "test", "USD", 2), 2)

		// Batch 2: delete the ledger, then index the recreated same-name ledger
		// in the SAME batch. The committed rows are still visible to a direct
		// Get, so the dedup must not let them suppress the new Put.
		batch2 := store.NewBatch()
		b.initBatch(batch2)
		b.wb.SetEventSequence(2)
		require.NoError(t, readstore.DeleteLedgerIndexes(b.wb.Batch(), "test"))
		b.markLedgerDeletedInBatch("test")
		require.NoError(t, b.indexPostingAddressMappings(
			b.kb, cfg, "test", 2, "accounts:alice", "accounts:carol", "USD/2", "",
			false, false, false, nil,
		))
		require.NoError(t, b.wb.Flush())

		// alice (re-touched) and carol (new) survive; bob existed only in the
		// deleted generation and is correctly gone (proving the range delete
		// fired while the recreate's writes were preserved).
		got := scanAccountByAsset(t, store, "test", "USD", 2)
		assert.Equal(t, map[string]struct{}{
			"accounts:alice": {},
			"accounts:carol": {},
		}, got)
	})

	t.Run("uncommitted same-batch row pending range-delete does not suppress the recreate", func(t *testing.T) {
		t.Parallel()

		store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
		require.NoError(t, err)

		defer func() { _ = store.Close() }()

		b := &Builder{
			readStore: store,
			kb:        dal.NewKeyBuilder(),
			wb:        readstore.NewWriteBatch(),
		}
		cfg := acctAssetConfig()

		batch := store.NewBatch()
		b.initBatch(batch)
		b.wb.SetEventSequence(1)

		// Old generation writes (queued, uncommitted) populate seenAcctAsset ...
		require.NoError(t, b.indexPostingAddressMappings(
			b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
			false, false, false, nil,
		))
		// ... the ledger is deleted in the same batch (range delete queued,
		// seenAcctAsset invalidated) ...
		require.NoError(t, readstore.DeleteLedgerIndexes(b.wb.Batch(), "test"))
		b.markLedgerDeletedInBatch("test")
		// ... and the recreated ledger re-touches alice. Without clearing
		// seenAcctAsset on delete, this Put would be skipped and lost.
		require.NoError(t, b.indexPostingAddressMappings(
			b.kb, cfg, "test", 2, "accounts:alice", "accounts:carol", "USD/2", "",
			false, false, false, nil,
		))
		require.NoError(t, b.wb.Flush())

		got := scanAccountByAsset(t, store, "test", "USD", 2)
		assert.Equal(t, map[string]struct{}{
			"accounts:alice": {},
			"accounts:carol": {},
		}, got)
	})
}

func TestIndexPostingAddressMappingsAccountByAssetExcludesTransient(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	cfg := acctAssetConfig()

	excludedVolumes := map[domain.AccountAssetKey]struct{}{
		{Account: "accounts:alice", Asset: "USD/2"}: {},
	}

	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
		false, false, false, excludedVolumes,
	))
	require.NoError(t, b.wb.Flush())

	got := scanAccountByAsset(t, store, "test", "USD", 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:bob": {},
	}, got)
}

func TestIndexPostingAddressMappingsAccountByAssetDisabled(t *testing.T) {
	t.Parallel()

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	b := &Builder{
		readStore: store,
		kb:        dal.NewKeyBuilder(),
		wb:        readstore.NewWriteBatch(),
	}

	batch := store.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)

	// Index NOT registered.
	cfg := newLedgerIndexConfig()

	require.NoError(t, b.indexPostingAddressMappings(
		b.kb, cfg, "test", 1, "accounts:alice", "accounts:bob", "USD/2", "",
		false, false, false, nil,
	))
	require.NoError(t, b.wb.Flush())

	got := scanAccountByAsset(t, store, "test", "USD", 2)
	assert.Empty(t, got)
}

func TestAppliedProposalSyncResumeSequenceOnlySkipsFullyConsumedRanges(t *testing.T) {
	t.Parallel()

	proposals := newTestAppliedProposalSync(
		testAppliedProposal(1, 1, 10, "ledger", "old:account"),
		testAppliedProposal(2, 20, 30, "ledger", "current:account"),
	)

	excluded := proposals.transientForLedger(25, "ledger")
	require.Contains(t, excluded, domain.AccountAssetKey{Account: "current:account", Asset: "USD"})
	assert.Equal(t, uint64(1), proposals.resumeSequence())

	proposals.advanceBefore(31)
	assert.Equal(t, uint64(2), proposals.resumeSequence())
}

func TestAppliedProposalSyncResumeSequenceKeepsInitialResumeFloor(t *testing.T) {
	t.Parallel()

	proposals := newTestAppliedProposalSyncAfter(1,
		testAppliedProposal(2, 20, 30, "ledger", "current:account"),
	)

	excluded := proposals.transientForLedger(25, "ledger")
	require.Contains(t, excluded, domain.AccountAssetKey{Account: "current:account", Asset: "USD"})
	assert.Equal(t, uint64(1), proposals.resumeSequence())
}

// TestAppliedProposalSyncSkipsAllIdempotentEntries guards the resume-advance
// path for AppliedProposal entries with MaxLogSequence == 0 (all-idempotent
// proposals: every order was a replay, no new log produced). The branch is
// load-bearing for cursor correctness across gaps — if it regressed and
// stopped advancing resumeAfterSeq on idempotent entries, a restart would
// replay already-consumed proposals.
func TestAppliedProposalSyncSkipsAllIdempotentEntries(t *testing.T) {
	t.Parallel()

	idempotent := &proposalpb.AppliedProposal{Sequence: 1, MinLogSequence: 0, MaxLogSequence: 0}

	proposals := newTestAppliedProposalSync(
		idempotent,
		testAppliedProposal(2, 20, 30, "ledger", "current:account"),
	)

	// Hitting a log inside the second (real) entry must still resolve
	// correctly even though the cursor advanced through an all-idempotent
	// entry on the way.
	excluded := proposals.transientForLedger(25, "ledger")
	require.Contains(t, excluded, domain.AccountAssetKey{Account: "current:account", Asset: "USD"})

	// The idempotent entry must have advanced the resume floor to its
	// sequence so a restart skips it instead of replaying.
	assert.GreaterOrEqual(t, proposals.resumeSequence(), uint64(1))
}

// TestAppliedProposalSyncFailsLoudlyWhenStreamExhaustedBeforeLog asserts
// the coverage invariant: a transientForLedger call for a log seq that
// falls beyond every entry in the stream stashes an error rather than
// returning nil silently. Otherwise the indexer would persist
// account->tx mappings on volumes that should have been excluded — a
// corruption-tolerated path.
func TestAppliedProposalSyncFailsLoudlyWhenStreamExhaustedBeforeLog(t *testing.T) {
	t.Parallel()

	proposals := newTestAppliedProposalSync(
		testAppliedProposal(1, 10, 15, "ledger", "transient:a"),
	)

	// Drain the only entry by asking about a log inside its range.
	_ = proposals.transientForLedger(12, "ledger")
	require.NoError(t, proposals.err(), "in-range query must not stash an error")

	// Now ask about a log past the end of the stream. There is no
	// AppliedProposal entry covering it — should-not-happen, must fail.
	excluded := proposals.transientForLedger(99, "ledger")
	require.Nil(t, excluded, "transient set must be nil on coverage error")
	require.Error(t, proposals.err())
	require.Contains(t, proposals.err().Error(), "applied proposal stream exhausted")
	require.Contains(t, proposals.err().Error(), "log 99")
}

// TestAppliedProposalSyncFailsLoudlyOnEmptyStream guards the empty-stream
// edge of the coverage invariant: if a log is being indexed but no
// AppliedProposal exists at all (corrupted/missing projection), the
// silent nil would mask the missing coverage.
func TestAppliedProposalSyncFailsLoudlyOnEmptyStream(t *testing.T) {
	t.Parallel()

	proposals := newTestAppliedProposalSync()

	excluded := proposals.transientForLedger(5, "ledger")
	require.Nil(t, excluded)
	require.Error(t, proposals.err())
	require.Contains(t, proposals.err().Error(), "applied proposal stream exhausted")
}

// TestAppliedProposalSyncFailsLoudlyWhenLogPrecedesCurrentRange asserts
// the second branch of the coverage invariant: if a logSeq lands in a
// gap *before* the current proposal's minLogSequence (after
// advanceBefore), it means the log has no covering proposal, which
// every successful proposal is supposed to provide. Fail loudly.
func TestAppliedProposalSyncFailsLoudlyWhenLogPrecedesCurrentRange(t *testing.T) {
	t.Parallel()

	// Single entry covering [10, 15]. Query for seq 5 — below the range.
	proposals := newTestAppliedProposalSync(
		testAppliedProposal(1, 10, 15, "ledger", "transient:a"),
	)

	excluded := proposals.transientForLedger(5, "ledger")
	require.Nil(t, excluded)
	require.Error(t, proposals.err())
	require.Contains(t, proposals.err().Error(), "falls in a gap before applied proposal range")
}

func newTestAppliedProposalSync(entries ...*proposalpb.AppliedProposal) *appliedProposalSync {
	return newTestAppliedProposalSyncAfter(0, entries...)
}

func newTestAppliedProposalSyncAfter(afterSeq uint64, entries ...*proposalpb.AppliedProposal) *appliedProposalSync {
	s := &appliedProposalSync{cursor: cursor.NewSliceCursor(entries)}
	s.resumeAfterSeq = afterSeq
	s.advance()

	return s
}

func testAppliedProposal(sequence, minLogSeq, maxLogSeq uint64, ledger string, accounts ...string) *proposalpb.AppliedProposal {
	// Test fixture: each "account" is paired with a default asset so the
	// (account, asset) granularity is exercised without requiring callers
	// to thread the asset everywhere.
	volumes := make([]*commonpb.TouchedVolume, len(accounts))
	for i, a := range accounts {
		volumes[i] = &commonpb.TouchedVolume{Account: a, Asset: "USD"}
	}

	return &proposalpb.AppliedProposal{
		Sequence:       sequence,
		MinLogSequence: minLogSeq,
		MaxLogSequence: maxLogSeq,
		TransientVolumes: map[string]*proposalpb.TouchedVolumeList{
			ledger: {Volumes: volumes},
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
// fix for an index desync during the backfill window. The incremental update
// deletes the old entry using the index's own reverse-map value. While the
// schema-rewrite backfill has not yet rewritten an entity, the index can still
// hold the pre-conversion raw String "30". A SavedMetadata carrying only the
// new Int64(40) must nevertheless retract that raw entry, leaving no stale
// membership.
func TestIndexSavedMetadata_OverwriteDeletesByReverseMapDuringBuilding(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.kb = dal.NewKeyBuilder()
	b.wb = readstore.NewWriteBatch()
	b.seedBatchSchema(t)

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

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(reverseKey, rawEncoded))
	require.NoError(t, seed.Commit())
	seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 1, rawEncoded, entityID, 1, readstore.MetadataEventAdd)

	// The "age" account index is still backfilling; incremental writes flow to it.
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{
		Id: id,
	}

	// The incremental log carries only the new age=40 value; the indexer
	// resolves the old encoded value via the reverse map.
	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(40)},
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	b.wb.SetEventSequence(2)
	require.NoError(t, b.indexSavedMetadata(kb, cfg, ledger, sm))
	require.NoError(t, b.wb.Flush())

	// New entry exists and the reverse map points at it.
	newEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(40))
	requireMetadataLive(t, b, ledger, readstore.NamespaceAccount, key, 1, newEncoded, entityID)
	assertReadStoreValue(t, b, reverseKey, newEncoded)

	// The pre-conversion entry is gone: the delete was located via the reverse
	// map (raw String "30"), not the log's coerced Int64(30), so a query for the
	// old value no longer returns acct-001.
	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 1, rawEncoded, entityID)
}

// TestIndexSavedMetadata_DualWritesDuringRewrite pins the EN-1323 step 4b
// contract: while a rewrite is in flight (pending_version != current),
// every live SavedMetadata is mirrored into BOTH the current and the
// pending forward/rmap keyspaces. The dual-write keeps v_current serving
// queries (entities that received updates after the retype stay
// reachable) and pre-populates v_pending so the rewrite's atomic switch
// promotes a fully-consistent view.
func TestIndexSavedMetadata_DualWritesDuringRewrite(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	const (
		ledger  = "test"
		account = "acct-101"
		key     = "score"
	)
	entityID := []byte(account)
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	// IndexVersionState: rewrite v=1 → v=2 in flight.
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(42)},
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, sm))
	require.NoError(t, b.wb.Flush())

	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))

	// Both versions must hold the entry.
	v1Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 1))
	v2Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 2))

	requireMetadataLive(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID)
	requireMetadataLive(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID)
	assertReadStoreValue(t, b, v1Rmap, encoded)
	assertReadStoreValue(t, b, v2Rmap, encoded)
}

// TestIndexDeletedMetadata_DualDeleteDuringRewrite mirrors the dual-write
// invariant for the delete path: a DeletedMetadata log while a rewrite is
// in flight must purge the entry from BOTH v_current and v_pending so the
// atomic switch doesn't promote a tombstoned entity back into existence.
func TestIndexDeletedMetadata_DualDeleteDuringRewrite(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	const (
		ledger  = "test"
		account = "acct-202"
		key     = "score"
	)
	entityID := []byte(account)
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))

	// Pre-seed both versions to mimic post-dual-write state.
	v1Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 1))
	v2Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 2))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(v1Rmap, encoded))
	require.NoError(t, seed.SetBytes(v2Rmap, encoded))
	require.NoError(t, seed.Commit())
	seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd)
	seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd)

	dm := &commonpb.DeletedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Key: key,
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	b.wb.SetEventSequence(2)
	require.NoError(t, b.indexDeletedMetadata(b.kb, cfg, ledger, dm))
	require.NoError(t, b.wb.Flush())

	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID)
	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID)
	assertReadStoreMissing(t, b, v1Rmap)
	assertReadStoreMissing(t, b, v2Rmap)
}

// TestIndexSavedMetadata_SingleWriteWhenNoRewrite confirms that the live
// path writes only to v_current when no rewrite is in flight — no wasted
// duplicate write at a pending version that doesn't exist.
func TestIndexSavedMetadata_SingleWriteWhenNoRewrite(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	const (
		ledger  = "test"
		account = "acct-303"
		key     = "score"
	)
	entityID := []byte(account)
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	// Steady state: current=1, no pending rewrite.
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 0,
	})

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(7)},
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, sm))
	require.NoError(t, b.wb.Flush())

	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(7))

	requireMetadataLive(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID)
	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID)
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
	b.seedBatchSchema(t)

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
	v1Encoded := readstore.EncodeMetadataValue(nil, v1)
	v2Encoded := readstore.EncodeMetadataValue(nil, v2)
	// Clone: KeyBuilder returns buffer-backed slices that later index calls reuse.
	reverseKey := cloneBytes(readstore.TransactionReverseMapKey(kb, ledger, txID, key))

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)

	// 1. CreateTransaction sets indexed tx metadata key=v1 (first write).
	b.wb.SetEventSequence(1)
	ct := &commonpb.CreatedTransaction{
		Transaction: &commonpb.Transaction{
			Id:       txID,
			Metadata: map[string]*commonpb.MetadataValue{key: v1},
		},
	}
	require.NoError(t, b.indexCreatedTransaction(kb, cfg, ledger, ct, nil))

	// 2. Same batch: overwrite the same key to v2 before the batch commits.
	b.wb.SetEventSequence(2)
	sm := &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_TransactionId{TransactionId: txID},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: v2},
	}
	require.NoError(t, b.indexSavedMetadata(kb, cfg, ledger, sm))

	require.NoError(t, b.wb.Flush())

	// v2 is indexed and the stale v1 entry from the create is gone.
	requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, key, 1, v2Encoded, txIDBytes)
	assertReadStoreValue(t, b, reverseKey, v2Encoded)
	requireMetadataDead(t, b, ledger, readstore.NamespaceTransaction, key, 1, v1Encoded, txIDBytes)
}

// TestNewTransactionMetadataUsesKnownAbsentInsert proves both production
// triggers bypass the reverse-map lookup. The pre-existing row is an
// intentionally unreachable sentinel: if either handler regresses to the
// replacement path it will observe the sentinel and emit a DEL for it. The
// known-absent insert must emit only its ADD and overwrite the reverse-map row.
func TestNewTransactionMetadataUsesKnownAbsentInsert(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "score"
	)

	tests := []struct {
		name  string
		txID  uint64
		index func(*Builder, *ledgerIndexConfig, *commonpb.MetadataValue) error
	}{
		{
			name: "CreatedTransaction",
			txID: 11,
			index: func(b *Builder, cfg *ledgerIndexConfig, value *commonpb.MetadataValue) error {
				return b.indexCreatedTransaction(b.kb, cfg, ledger, &commonpb.CreatedTransaction{
					Transaction: &commonpb.Transaction{
						Id:       11,
						Metadata: map[string]*commonpb.MetadataValue{key: value},
					},
				}, nil)
			},
		},
		{
			name: "RevertedTransaction",
			txID: 12,
			index: func(b *Builder, cfg *ledgerIndexConfig, value *commonpb.MetadataValue) error {
				return b.indexRevertedTransaction(b.kb, cfg, ledger, &commonpb.RevertedTransaction{
					RevertTransaction: &commonpb.Transaction{
						Id:       12,
						Metadata: map[string]*commonpb.MetadataValue{key: value},
					},
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			b.accounts = make(map[string]struct{})
			b.seedBatchSchema(t)

			cfg := newLedgerIndexConfig()
			id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)
			canonical := indexes.Canonical(id)
			cfg.byCanonical[canonical] = &commonpb.Index{Id: id}
			b.putVersionState(ledger, canonical, readstore.IndexVersionState{
				CurrentVersion: 1,
				PendingVersion: 2,
			})

			oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("sentinel"))
			newValue := commonpb.NewStringValue("new")
			newEncoded := readstore.EncodeMetadataValue(nil, newValue)
			entityID := readstore.EncodeTxID(make([]byte, 0, 8), tt.txID)
			v1Rmap := cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledger, tt.txID, key, 1))
			v2Rmap := cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledger, tt.txID, key, 2))

			seed := b.readStore.NewBatch()
			require.NoError(t, seed.SetBytes(v1Rmap, oldEncoded))
			require.NoError(t, seed.SetBytes(v2Rmap, oldEncoded))
			require.NoError(t, seed.Commit())
			seedMetadataEvent(t, b, ledger, readstore.NamespaceTransaction, key, 1, oldEncoded, entityID, 1, readstore.MetadataEventAdd)
			seedMetadataEvent(t, b, ledger, readstore.NamespaceTransaction, key, 2, oldEncoded, entityID, 1, readstore.MetadataEventAdd)

			batch := b.readStore.NewBatch()
			b.wb.Init(batch)
			b.wb.SetEventSequence(2)
			require.NoError(t, tt.index(b, cfg, newValue))
			require.NoError(t, b.wb.Flush())

			// The sentinel remains live only when no reverse-map lookup and no
			// replacement DEL occurred. The real lifecycle excludes this double
			// membership; here it is solely an observable read probe.
			for _, version := range []uint32{1, 2} {
				requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, key, version, oldEncoded, entityID)
				requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, key, version, newEncoded, entityID)
				assert.Equal(t, 2, countEventsAt(t, b.readStore, ledger, readstore.NamespaceTransaction, key, version))
			}
			assertReadStoreValue(t, b, v1Rmap, newEncoded)
			assertReadStoreValue(t, b, v2Rmap, newEncoded)
		})
	}
}

func TestDualInsertKnownAbsentMetadataIndexPropagatesFailures(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "score"
		txID   = uint64(23)
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)
	canonical := indexes.Canonical(id)
	value := commonpb.NewStringValue("new")
	entityID := readstore.EncodeTxID(make([]byte, 0, 8), txID)

	t.Run("current version binding failure", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.putVersionState(ledger, canonical, readstore.IndexVersionState{CurrentVersion: 1})
		b.seedActiveBatch(t)
		b.wb.SetEventSequence(1)

		err := b.dualInsertKnownAbsentMetadataIndex(
			b.kb,
			ledger, readstore.NamespaceTransaction, key,
			commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			value, entityID,
			func(version uint32) []byte {
				// Simulate the impossible wiring race guarded by
				// coerceForVersion: the selected current version is no
				// longer bound when the insert reaches it.
				b.putVersionState(ledger, canonical, readstore.IndexVersionState{CurrentVersion: 2})

				return readstore.TransactionReverseMapKeyV(b.kb, ledger, txID, key, version)
			},
		)

		require.ErrorContains(t, err, "invariant: write at version 1")
	})

	t.Run("pending write failure", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.putVersionState(ledger, canonical, readstore.IndexVersionState{
			CurrentVersion: 1,
			PendingVersion: 2,
		})

		batch := b.readStore.NewBatch()
		b.wb.Init(batch)
		b.wb.SetEventSequence(1)
		t.Cleanup(b.wb.Reset)

		err := b.dualInsertKnownAbsentMetadataIndex(
			b.kb,
			ledger, readstore.NamespaceTransaction, key,
			commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			value, entityID,
			func(version uint32) []byte {
				if version == 2 {
					// Commit after the current-version write so the pending
					// version deterministically exercises write-error
					// propagation without a production fault hook.
					require.NoError(t, batch.Commit())
				}

				return readstore.TransactionReverseMapKeyV(b.kb, ledger, txID, key, version)
			},
		)

		require.ErrorContains(t, err, "write session already committed")
	})
}

func TestAddSchemaRewriteTaskBackfillResetPropagatesBatchFailures(t *testing.T) {
	t.Parallel()

	const (
		ledger = "test"
		key    = "score"
	)

	newBuilder := func(t *testing.T) *Builder {
		t.Helper()

		b := newTestBuilderWithStore(t)
		b.backfillTasks = []*backfillTask{{
			ledger: ledger,
			index:  indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key),
			cursor: 77,
			bbKey:  backfillBBKey(ledger, indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)),
		}}

		return b
	}

	schemaLog := &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		Key:        key,
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
	}

	t.Run("missing active batch", func(t *testing.T) {
		t.Parallel()

		b := newBuilder(t)
		err := b.addSchemaRewriteTask(newLedgerIndexConfig(), ledger, schemaLog)

		require.ErrorContains(t, err, "invariant: addSchemaRewriteTask called without an active write batch")
		assert.Equal(t, uint64(77), b.backfillTasks[0].cursor)
	})

	t.Run("cursor delete failure", func(t *testing.T) {
		t.Parallel()

		b := newBuilder(t)
		batch := b.readStore.NewBatch()
		require.NoError(t, batch.Commit())
		b.wb.Init(batch)
		t.Cleanup(b.wb.Reset)

		err := b.addSchemaRewriteTask(newLedgerIndexConfig(), ledger, schemaLog)

		require.ErrorContains(t, err, "resetting persisted backfill cursor on retype: write session already committed")
		assert.Equal(t, uint64(77), b.backfillTasks[0].cursor)
	})
}

func TestProcessSchemaRewriteCountsScannedKeysAgainstBudgetAndPersistsCursor(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})
	ledgerName := "test"

	// Per-replica IndexVersionState as set by addSchemaRewriteTask: this
	// rewrite migrates v=1 → v=2 for (account, "status").
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"))
	b.putVersionState(ledgerName, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	firstSkippedKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-001", "other")
	secondSkippedKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-002", "other")
	matchingKey := readstore.AccountReverseMapKey(kb, ledgerName, "acct-003", "status")

	skippedEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("ignored"))
	oldEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("42"))
	newEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))
	entityID := []byte("acct-003")

	// v=2 rmap entry (target of the rewrite).
	newRmapKeyV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, "acct-003", "status", 2))

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(firstSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(secondSkippedKey, skippedEncoded))
	require.NoError(t, batch.SetBytes(matchingKey, oldEncoded))
	require.NoError(t, batch.Commit())

	// v=1 forward entry (the pre-retype state). The rewrite no longer
	// touches v=1 (in-place mutation is gone); v=1 stays until GC.
	seedMetadataEvent(t, b, ledgerName, readstore.NamespaceAccount, "status", 1, oldEncoded, entityID, 1, readstore.MetadataEventAdd)

	// The rewrite stamps its events with the FSM log sequence it samples and
	// gates the atomic switch on the read store having indexed up to it.
	seedRewriteSequence(t, b, 2)

	// Seed the FSM-side canonical stored value for acct-003.status. The schema
	// rewrite reads from here, not from the rmap, so re-encoding is a pure
	// function of immutable stored state.
	fsmBatch := b.pebbleStore.OpenWriteSession()
	canonicalKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "acct-003"},
		Key:        "status",
	}.Bytes()
	_, err := b.attrs.Metadata.Set(fsmBatch, canonicalKey, commonpb.NewStringValue("42"))
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

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
	requireMetadataLive(t, b, ledgerName, readstore.NamespaceAccount, "status", 1, oldEncoded, entityID)
	assertReadStoreValue(t, b, matchingKey, oldEncoded)

	done, err = b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, done)
	assert.Equal(t, uint64(1), task.processedCount)
	assert.Equal(t, matchingKey, task.rmapCursor)

	// Atomic switch GCs v_old in the same batch: the v=1 forward
	// entry and the v=1 rmap row are gone; v=2 forward and rmap are
	// populated by the rewrite.
	requireMetadataDead(t, b, ledgerName, readstore.NamespaceAccount, "status", 1, oldEncoded, entityID)
	requireMetadataLive(t, b, ledgerName, readstore.NamespaceAccount, "status", 2, newEncoded, entityID)
	assertReadStoreMissing(t, b, matchingKey)
	assertReadStoreValue(t, b, newRmapKeyV2, newEncoded)

	// Atomic switch promoted current ← pending; queries now read v=2.
	current, pending := b.versionFor(ledgerName, canonical)
	assert.Equal(t, uint32(2), current)
	assert.Equal(t, uint32(0), pending)
}

func TestProcessSchemaRewriteStopsBeforeScanningWhenStopClosed(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})
	close(stop)

	ledgerName := "test"

	// Seed the per-replica version state so processSchemaRewrite has a
	// well-formed (current, pending) pair to consult. The stop signal
	// fires before any rewrite work happens so this state will be
	// unchanged after the call.
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"))
	b.putVersionState(ledgerName, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

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

	// Atomic switch did NOT fire — task was stopped before any work.
	current, pending := b.versionFor(ledgerName, canonical)
	assert.Equal(t, uint32(1), current)
	assert.Equal(t, uint32(2), pending)
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
			name:     "destination address index",
			id:       indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS),
			expected: true,
		},
		{
			name:     "account has-asset index",
			id:       indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
			expected: true,
		},
		{
			name:     "account builtin unspecified (not posting)",
			id:       indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED),
			expected: false,
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

// TestProcessSchemaRewrite_LosslessRoundTrip pins the headline property of
// the FSM-sourced rewrite: even when re-encoding takes a value through a
// type that loses information (STRING "030" → UINT64 30), running the
// rewrite a second time targeting STRING returns to the original "030"
// because the canonical stored value in the FSM was never mutated.
//
// Versioned form: each rewrite writes into a fresh v_pending keyspace.
// The first rewrite migrates v=1 → v=2 (STRING → UINT64). The second
// then migrates v=2 → v=3 (UINT64 → STRING). The FSM raw value
// ("030") drives both re-encodings, so the leading zero survives the
// round trip.
func TestProcessSchemaRewrite_LosslessRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})

	const (
		ledgerName = "test"
		account    = "users:001"
		key        = "score"
	)
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	// Seed FSM canonical stored value: STRING "030" (immutable through the
	// whole test — only the indexer's encoding view changes).
	fsmBatch := b.pebbleStore.OpenWriteSession()
	canonicalKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Key:        key,
	}.Bytes()
	_, err := b.attrs.Metadata.Set(fsmBatch, canonicalKey, commonpb.NewStringValue("030"))
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	// Seed rmap + forward index in the STRING encoding (state before any
	// retype). This is what the indexer would have written when the field
	// was STRING-typed.
	stringEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("030"))
	entityID := []byte(account)
	reverseKeyV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, account, key, 1))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(reverseKeyV1, stringEncoded))
	require.NoError(t, seed.Commit())
	seedMetadataEvent(t, b, ledgerName, readstore.NamespaceAccount, key, 1, stringEncoded, entityID, 1, readstore.MetadataEventAdd)

	// The rewrite stamps its events with the FSM log sequence it samples and
	// gates the atomic switch on the read store having indexed up to it.
	seedRewriteSequence(t, b, 2)

	// First rewrite: STRING (v=1) → UINT64 (v=2).
	b.putVersionState(ledgerName, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	task := &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        key,
		toType:     commonpb.MetadataType_METADATA_TYPE_UINT64,
		bbKey:      schemaRewriteBBKey(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key),
	}
	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, done)

	uint64Encoded := readstore.EncodeMetadataValue(nil, commonpb.NewUintValue(30))
	reverseKeyV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, account, key, 2))

	requireMetadataLive(t, b, ledgerName, readstore.NamespaceAccount, key, 2, uint64Encoded, entityID)
	assertReadStoreValue(t, b, reverseKeyV2, uint64Encoded)
	// Atomic switch GC purges v=1 in the same batch as the version
	// promotion — the pre-retype forward entry is gone.
	requireMetadataDead(t, b, ledgerName, readstore.NamespaceAccount, key, 1, stringEncoded, entityID)

	current, pending := b.versionFor(ledgerName, canonical)
	require.Equal(t, uint32(2), current)
	require.Equal(t, uint32(0), pending)

	// Second rewrite: UINT64 (v=2) → STRING (v=3). The new encoding
	// sources from the raw stored STRING "030", NOT from the uint64(30)
	// currently in the v=2 rmap — so the leading zero is preserved.
	b.putVersionState(ledgerName, canonical, readstore.IndexVersionState{
		CurrentVersion: 2,
		PendingVersion: 3,
	})

	task2 := &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        key,
		toType:     commonpb.MetadataType_METADATA_TYPE_STRING,
		bbKey:      schemaRewriteBBKey(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key),
	}
	done, err = b.processSchemaRewrite(task2, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, done)

	reverseKeyV3 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, account, key, 3))

	requireMetadataLive(t, b, ledgerName, readstore.NamespaceAccount, key, 3, stringEncoded, entityID)
	assertReadStoreValue(t, b, reverseKeyV3, stringEncoded)
	// v=2 forward + rmap (the previous "current") are GC'd by the
	// second switch.
	requireMetadataDead(t, b, ledgerName, readstore.NamespaceAccount, key, 2, uint64Encoded, entityID)
	assertReadStoreMissing(t, b, reverseKeyV2)

	current, pending = b.versionFor(ledgerName, canonical)
	assert.Equal(t, uint32(3), current)
	assert.Equal(t, uint32(0), pending)
}

// TestProcessSchemaRewrite_SkipsUncoercibleAsNullSentinel pins behavior for
// raw stored values that cannot be coerced to the new declared type: the
// forward index entry is keyed by the Null sentinel encoding (which
// preserves the original string), letting range queries cleanly skip the
// entity while still surfacing it on direct reads.
func TestProcessSchemaRewrite_SkipsUncoercibleAsNullSentinel(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	stop := make(chan struct{})

	const (
		ledgerName = "test"
		account    = "users:002"
		key        = "score"
	)

	// FSM holds a STRING that cannot be parsed as uint64.
	fsmBatch := b.pebbleStore.OpenWriteSession()
	canonicalKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Key:        key,
	}.Bytes()
	_, err := b.attrs.Metadata.Set(fsmBatch, canonicalKey, commonpb.NewStringValue("abc"))
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	stringEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("abc"))
	entityID := []byte(account)
	reverseKeyV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, account, key, 1))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(reverseKeyV1, stringEncoded))
	require.NoError(t, seed.Commit())
	seedMetadataEvent(t, b, ledgerName, readstore.NamespaceAccount, key, 1, stringEncoded, entityID, 1, readstore.MetadataEventAdd)

	// The rewrite stamps its events with the FSM log sequence it samples and
	// gates the atomic switch on the read store having indexed up to it.
	seedRewriteSequence(t, b, 2)

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))
	b.putVersionState(ledgerName, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	task := &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        key,
		toType:     commonpb.MetadataType_METADATA_TYPE_UINT64,
		bbKey:      schemaRewriteBBKey(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key),
	}
	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, done)

	// v=2 forward is keyed by the Null sentinel for "abc". Atomic
	// switch GC purges v=1 in the same batch as the version
	// promotion, so the pre-retype forward entry is gone.
	nullEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewNullValue("abc"))
	reverseKeyV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledgerName, account, key, 2))

	requireMetadataDead(t, b, ledgerName, readstore.NamespaceAccount, key, 1, stringEncoded, entityID)
	requireMetadataLive(t, b, ledgerName, readstore.NamespaceAccount, key, 2, nullEncoded, entityID)
	assertReadStoreValue(t, b, reverseKeyV2, nullEncoded)

	current, pending := b.versionFor(ledgerName, canonical)
	assert.Equal(t, uint32(2), current)
	assert.Equal(t, uint32(0), pending)
}

// TestSchemaRewrite_SeqGate_DefersSwitchWhenReadStoreLags pins the F3
// contract: when the rewrite scan exhausts with a non-zero
// requiredIndexedSeq watermark that the read-store cursor hasn't
// reached yet, the atomic switch must be DEFERRED until
// LastIndexedSequence catches up. Without this gate, the post-switch
// v_new keyspace would serve rows derived from FSM log seq > cursor,
// breaking the contiguous-prefix invariant min_log_sequence-gated
// queries rely on.
//
// Setup short-circuits the scan via scanComplete=true so the test
// exercises only the gate logic; the scan path's sampling of
// query.ReadLastSequence is covered by the lossless retype round-trip
// tests (which exhaust the scan and fire the same-batch switch).
func TestSchemaRewrite_SeqGate_DefersSwitchWhenReadStoreLags(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	stop := make(chan struct{})

	const ledger = "test"

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"))
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	task := &schemaRewriteTask{
		ledger:             ledger,
		targetType:         commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:                "status",
		toType:             commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:              schemaRewriteBBKey(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
		scanComplete:       true,
		requiredIndexedSeq: 100, // rewrite observed FSM at log seq 100
	}

	// Read store cursor is behind the FSM watermark.
	progressBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(progressBatch, 50))
	require.NoError(t, progressBatch.Commit())

	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, done,
		"switch must be deferred when LastIndexedSequence (50) < requiredIndexedSeq (100) — otherwise the v_new keyspace would serve rows ahead of the read-store cursor")

	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(1), current,
		"current_version must NOT advance while the seq gate is closed (got %d, want 1)", current)
	assert.Equal(t, uint32(2), pending,
		"pending_version must stay set so the task survives to the next tick (got %d, want 2)", pending)
	assert.True(t, task.scanComplete,
		"scanComplete must remain true so the next tick routes through tryCommitScanCompleteSwitch")

	// Read-store cursor catches up to (past) the watermark.
	progressBatch2 := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(progressBatch2, 100))
	require.NoError(t, progressBatch2.Commit())

	done, err = b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, done,
		"switch must fire once LastIndexedSequence (100) >= requiredIndexedSeq (100)")

	current, pending = b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(2), current,
		"current_version must advance to pending after the gate releases (got %d, want 2)", current)
	assert.Equal(t, uint32(0), pending,
		"pending_version must clear after the switch (got %d, want 0)", pending)
}

// TestSchemaRewrite_SeqGate_SameBatchSwitchWhenGateMet pins the
// performance-side contract: when the seq gate is already met at
// scan-exhaust time (LastIndexedSequence >= requiredIndexedSeq), the
// switch + GC commit in the SAME batch as the last v_pending writes
// — no extra batch needed. This is the common steady-state path and
// the test guards against accidentally always-splitting into two
// commits.
func TestSchemaRewrite_SeqGate_SameBatchSwitchWhenGateMet(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	stop := make(chan struct{})

	const ledger = "test"

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"))
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	// Read store cursor already at/past whatever FSM watermark the
	// rewrite samples — sufficient because the FSM Pebble in
	// newTestBuilderWithStore is fresh (no log entries written), so
	// ReadLastSequence returns 0 and the gate fires immediately.
	progressBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(progressBatch, 1))
	require.NoError(t, progressBatch.Commit())

	task := &schemaRewriteTask{
		ledger:     ledger,
		targetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:        "status",
		toType:     commonpb.MetadataType_METADATA_TYPE_INT64,
		bbKey:      schemaRewriteBBKey(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status"),
	}

	// Empty rmap → scan exhausts immediately on the first call. With
	// the gate already met (FSM seq 0, readStore cursor 1), the switch
	// fires in the same batch and the task retires in one shot.
	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, done, "single-call retirement is the happy-path contract — gate met means same-batch switch")
	assert.True(t, task.scanComplete)

	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(2), current)
	assert.Equal(t, uint32(0), pending)
}

// writeLogToFSM persists a marshaled Log under [ZoneHistory][SubHistoryLog][seq] so
// the backfill iterator (query.ReadLogsSinceRaw) can read it back.
func writeLogToFSM(t *testing.T, b *Builder, log *commonpb.Log) {
	t.Helper()

	data, err := log.MarshalVT()
	require.NoError(t, err)

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(log.GetSequence())

	session := b.pebbleStore.OpenWriteSession()
	require.NoError(t, session.SetBytes(kb.Build(), data))
	require.NoError(t, session.Commit())
}

// writeAppliedProposalToFSM persists an AppliedProposal covering [min,max] log
// sequences with no transient volumes — the backfill posting walk consults the
// AppliedProposal stream for exclusions and fails loudly on coverage gaps.
func writeAppliedProposalToFSM(t *testing.T, b *Builder, seq, minLog, maxLog uint64) {
	t.Helper()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).PutUint64(seq)

	session := b.pebbleStore.OpenWriteSession()
	require.NoError(t, session.SetProto(kb.Build(), &proposalpb.AppliedProposal{
		Sequence:       seq,
		MinLogSequence: minLog,
		MaxLogSequence: maxLog,
	}))
	require.NoError(t, session.Commit())
}

// TestAccountAssetBackfillLifecycle exercises the end-to-end PENDING →
// build lifecycle for an ACCT_BUILTIN_INDEX_ASSET index:
// CreateIndex schedules a backfill, the driver replays historical
// CreatedTransaction / RevertedTransaction logs through the shared posting
// walk with the account-asset index registered, and on catch-up the index's
// version flips current ← pending while AccountByAssetPrefix scans
// return the historical accounts.
func TestAccountAssetBackfillLifecycle(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const ledger = "test"

	// Historical logs: two CreatedTransaction touching USD/2, one
	// RevertedTransaction touching EUR/2. AppliedProposals cover each.
	writeLogToFSM(t, b, makeCreatedTxLog(1, ledger, 100, []*commonpb.Posting{
		{Source: "accounts:alice", Destination: "accounts:bob", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	writeLogToFSM(t, b, makeCreatedTxLog(2, ledger, 101, []*commonpb.Posting{
		{Source: "accounts:bob", Destination: "accounts:carol", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 2, 2, 2)

	writeLogToFSM(t, b, makeRevertedTxLog(3, ledger, 100, 102, []*commonpb.Posting{
		{Source: "accounts:dave", Destination: "accounts:erin", Asset: "EUR/2"},
	}))
	writeAppliedProposalToFSM(t, b, 3, 3, 3)

	globalCursor, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(3), globalCursor)

	canonical := indexes.Canonical(indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET))

	// CreateIndex: registers the index, seeds {current:0, pending:1}, schedules
	// the backfill task. Wrap in an active batch so the IndexVersionState
	// persistence inside handleCreatedIndexLog has a batch to write into.
	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{
		Id: indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
	}))
	require.NoError(t, b.wb.Flush())

	require.Len(t, b.backfillTasks, 1, "CreateIndex must schedule one account-asset backfill task")

	// PENDING: current=0, pending=1 before the backfill runs.
	current, pending := b.versionFor(ledger, canonical)
	require.Equal(t, uint32(0), current, "index must be PENDING (current=0) before backfill catches up")
	require.Equal(t, uint32(1), pending)

	// Drive the backfill until it catches up to the global cursor. Generous
	// budget so a single tick replays all three logs.
	b.backfillBudget = time.Second
	stop := make(chan struct{})
	for range 10 {
		if len(b.backfillTasks) == 0 {
			break
		}
		b.processBackfills(context.Background(), stop, globalCursor)
	}

	require.Empty(t, b.backfillTasks, "backfill task must be retired after catch-up")

	// Built: the atomic switch promoted current ← pending.
	current, pending = b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(1), current, "index must be READY (current=pending) after backfill catches up")
	assert.Equal(t, uint32(0), pending)

	// The historical accounts are now reachable through AccountByAssetPrefix.
	gotUSD := scanAccountByAsset(t, b.readStore, ledger, "USD", 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:alice": {},
		"accounts:bob":   {},
		"accounts:carol": {},
	}, gotUSD)

	gotEUR := scanAccountByAsset(t, b.readStore, ledger, "EUR", 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:dave": {},
		"accounts:erin": {},
	}, gotEUR)
}

// TestAccountAssetBackfillWipesDeletedLedgerGeneration pins that the
// account-asset backfill honors a DeleteLedger log replayed mid-stream. The
// backfill replays the global log from the start with no generation filter, so
// a delete + same-name recreate would otherwise leave the deleted generation's
// account-by-asset rows mixed under the shared by-name keyspace. The replayed
// DeleteLedger must wipe them (via DeleteLedgerIndexes) before the recreated
// generation is indexed, mirroring the live processLogs path.
func TestAccountAssetBackfillWipesDeletedLedgerGeneration(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const ledger = "test"

	// Generation 1: alice & bob touch USD/2.
	writeLogToFSM(t, b, makeCreatedTxLog(1, ledger, 100, []*commonpb.Posting{
		{Source: "accounts:alice", Destination: "accounts:bob", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	// DeleteLedger: the entire generation-1 keyspace must be wiped on replay.
	writeLogToFSM(t, b, makeDeleteLedgerLog(2, ledger))
	writeAppliedProposalToFSM(t, b, 2, 2, 2)

	// Generation 2 (same name, recreated): only carol & dave touch USD/2.
	writeLogToFSM(t, b, makeCreatedTxLog(3, ledger, 101, []*commonpb.Posting{
		{Source: "accounts:carol", Destination: "accounts:dave", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 3, 3, 3)

	globalCursor, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(3), globalCursor)

	canonical := indexes.Canonical(indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET))

	// CreateIndex schedules the backfill (current=0, pending=1).
	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{
		Id: indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
	}))
	require.NoError(t, b.wb.Flush())
	require.Len(t, b.backfillTasks, 1, "CreateIndex must schedule one account-asset backfill task")

	// Drive the backfill to catch-up. A generous budget replays all three
	// logs (gen-1 write, delete, gen-2 write) in a single batch, so the range
	// delete and the recreate's writes land in one commit.
	b.backfillBudget = time.Second
	stop := make(chan struct{})
	for range 10 {
		if len(b.backfillTasks) == 0 {
			break
		}
		b.processBackfills(context.Background(), stop, globalCursor)
	}
	require.Empty(t, b.backfillTasks, "backfill task must be retired after catch-up")

	// Built: the atomic switch promoted current ← pending.
	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(1), current)
	assert.Equal(t, uint32(0), pending)

	// Only the recreated generation's accounts survive. alice & bob existed
	// only in the deleted generation and must be gone — proving the replayed
	// DeleteLedger fired and its range delete did not clobber the recreate's
	// writes ordered after it.
	got := scanAccountByAsset(t, b.readStore, ledger, "USD", 2)
	assert.Equal(t, map[string]struct{}{
		"accounts:carol": {},
		"accounts:dave":  {},
	}, got)
}

// TestAccountAssetBackfillDoesNotWipeUnrelatedLedger pins that a single-ledger
// account-asset backfill, which replays the GLOBAL log, must not act on a
// DeleteLedger entry for a *different* ledger. DeleteLedgerIndexes is a full
// wipe of every ledger-scoped prefix (version state, backfill state, all index
// keyspaces), so firing it for an unrelated, currently-live ledger during this
// task's replay would silently degrade that ledger's indexes (version reset to
// 0, rows gone). Regression for the EN-1368 review blocker.
func TestAccountAssetBackfillDoesNotWipeUnrelatedLedger(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const (
		other = "other"
		task  = "task"
	)

	canonical := indexes.Canonical(indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET))

	// Single global stream. Ledger "other" lives through a delete + same-name
	// recreate (so it currently holds live state), then "task" records its own
	// transaction:
	//   seq 1: other gen-1 (alice/bob)
	//   seq 2: DeleteLedger(other)
	//   seq 3: other gen-2 (carol/dave) -- the surviving generation
	//   seq 4: task (zoe/yan)
	writeLogToFSM(t, b, makeCreatedTxLog(1, other, 100, []*commonpb.Posting{
		{Source: "accounts:alice", Destination: "accounts:bob", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	writeLogToFSM(t, b, makeDeleteLedgerLog(2, other))
	writeAppliedProposalToFSM(t, b, 2, 2, 2)

	writeLogToFSM(t, b, makeCreatedTxLog(3, other, 101, []*commonpb.Posting{
		{Source: "accounts:carol", Destination: "accounts:dave", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 3, 3, 3)

	writeLogToFSM(t, b, makeCreatedTxLog(4, task, 102, []*commonpb.Posting{
		{Source: "accounts:zoe", Destination: "accounts:yan", Asset: "USD/2"},
	}))
	writeAppliedProposalToFSM(t, b, 4, 4, 4)

	globalCursor, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(4), globalCursor)

	// Bring "other"'s account-asset index live first. Its own backfill
	// honors the mid-stream DeleteLedger(other), so only the recreated
	// generation survives.
	runAccountAssetBackfill(t, b, other, globalCursor)

	// "other"'s version state is persisted to Pebble as {current:1, pending:0}.
	// This is the load-bearing signal: a node restart rebuilds the in-memory
	// version cache from this row (initIndexConfig → ReadAllIndexVersionStates),
	// so wiping it silently degrades "other"'s READY index to "still building".
	otherState, found, err := b.readStore.ReadIndexVersionState(other, canonical)
	require.NoError(t, err)
	require.True(t, found, "other's version state must be persisted before task's backfill runs")
	require.Equal(t, uint32(1), otherState.CurrentVersion)
	require.Equal(t, uint32(0), otherState.PendingVersion)
	require.Equal(t, map[string]struct{}{
		"accounts:carol": {},
		"accounts:dave":  {},
	}, scanAccountByAsset(t, b.readStore, other, "USD", 2))

	// Now build "task"'s account-asset index. Its backfill replays the SAME
	// global log and encounters DeleteLedger(other) at seq 2 — it must skip it,
	// because "other" is unrelated to this task and currently READY.
	runAccountAssetBackfill(t, b, task, globalCursor)

	// task is indexed from its own transaction.
	assert.Equal(t, map[string]struct{}{
		"accounts:zoe": {},
		"accounts:yan": {},
	}, scanAccountByAsset(t, b.readStore, task, "USD", 2))

	// "other"'s PERSISTED version state is untouched. Before the fix, task's
	// backfill called DeleteLedgerIndexes(other), range-deleting other's
	// SubInternalIndexVersion row — so on the next restart other's READY index
	// would silently revert to current=0. (The in-memory versionFor and the
	// account-by-asset rows are NOT reliable signals here: the cache is not
	// re-read mid-process, and the buggy global writes re-create other's rows
	// after the range delete within the same batch — only the persisted version
	// state exposes the wipe.)
	otherState, found, err = b.readStore.ReadIndexVersionState(other, canonical)
	require.NoError(t, err)
	assert.True(t, found, "task's backfill must not wipe other's persisted version state")
	assert.Equal(t, uint32(1), otherState.CurrentVersion, "task's backfill must not reset other's index version")
	assert.Equal(t, uint32(0), otherState.PendingVersion)
	assert.Equal(t, map[string]struct{}{
		"accounts:carol": {},
		"accounts:dave":  {},
	}, scanAccountByAsset(t, b.readStore, other, "USD", 2))
}

// runAccountAssetBackfill creates the account-asset index for ledger and drives
// its backfill to catch up to globalCursor, leaving the index READY.
func runAccountAssetBackfill(t *testing.T, b *Builder, ledger string, globalCursor uint64) {
	t.Helper()

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{
		Id: indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
	}))
	require.NoError(t, b.wb.Flush())

	b.backfillBudget = time.Second
	stop := make(chan struct{})
	for range 10 {
		if len(b.backfillTasks) == 0 {
			break
		}
		b.processBackfills(context.Background(), stop, globalCursor)
	}
	require.Empty(t, b.backfillTasks, "backfill task for %q must be retired after catch-up", ledger)
}

// mustReadHandle opens a direct read handle on the builder's FSM store and
// registers its cleanup with the test.
func mustReadHandle(t *testing.T, b *Builder) *dal.ReadHandle {
	t.Helper()

	handle, err := b.pebbleStore.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	return handle
}

// makeSavedAccountMetadataLog builds a standalone account-metadata write log.
func makeSavedAccountMetadataLog(seq uint64, ledger, account, key, value string) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id: seq,
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: account},
										},
									},
									Metadata: map[string]*commonpb.MetadataValue{
										key: commonpb.NewStringValue(value),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// countKeysWithPrefix returns how many readstore keys live under prefix.
func countKeysWithPrefix(t *testing.T, store *readstore.Store, prefix []byte) int {
	t.Helper()

	iter, err := store.DB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}

	require.NoError(t, iter.Error())

	return n
}

// TestMetadataBackfillSkipsForeignLedgerLogs pins the generic backfill
// driver's ledger scoping: processBackfill replays the GLOBAL log with a
// temporary config holding the task's index, and indexLogEntry keys writes
// by each log's own ledger — so a foreign ledger's SavedMetadata on the same
// key would pass the config gates and write forward+rmap rows into the
// foreign keyspace for a field that ledger never indexed, surfacing as
// reverse-map orphans in compareReverseMapOrphans (EN-1458). The postings
// driver carries the same guard, pinned by
// TestAccountAssetBackfillDoesNotWipeUnrelatedLedger.
func TestMetadataBackfillSkipsForeignLedgerLogs(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	const (
		taskLedger    = "task"
		foreignLedger = "other"
		metaKey       = "leak"
	)

	writeLogToFSM(t, b, makeSavedAccountMetadataLog(1, taskLedger, "accounts:own", metaKey, "v1"))
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	writeLogToFSM(t, b, makeSavedAccountMetadataLog(2, foreignLedger, "accounts:foreign", metaKey, "v2"))
	writeAppliedProposalToFSM(t, b, 2, 2, 2)

	globalCursor, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(2), globalCursor)

	// CreateIndex on (ACCOUNT, metaKey) in taskLedger only.
	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.handleCreatedIndexLog(taskLedger, &commonpb.CreatedIndexLog{
		Id: indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey),
	}))
	require.NoError(t, b.wb.Flush())
	require.Len(t, b.backfillTasks, 1, "CreateIndex must schedule one metadata backfill task")

	b.backfillBudget = time.Second
	stop := make(chan struct{})
	for range 10 {
		if len(b.backfillTasks) == 0 {
			break
		}
		b.processBackfills(context.Background(), stop, globalCursor)
	}
	require.Empty(t, b.backfillTasks, "backfill task must be retired after catch-up")

	// The task ledger's rows were backfilled — guards against this test
	// passing vacuously on a backfill that indexed nothing at all.
	taskRmap := readstore.ReverseMapPrefix(dal.NewKeyBuilder(), taskLedger, readstore.NamespaceAccount)
	assert.NotZero(t, countKeysWithPrefix(t, b.readStore, taskRmap), "task ledger's rmap rows must be backfilled")

	// The foreign ledger's keyspace stayed untouched: it never indexed
	// anything, so any row here is an orphan.
	foreignRmap := readstore.ReverseMapPrefix(dal.NewKeyBuilder(), foreignLedger, readstore.NamespaceAccount)
	assert.Zero(t, countKeysWithPrefix(t, b.readStore, foreignRmap), "foreign ledger must have no rmap rows")
}
