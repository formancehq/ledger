package indexbuilder

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// countEventsAt counts event keys under one versioned metadata-index prefix.
func countEventsAt(t *testing.T, s *readstore.Store, ledger, ns, key string, version uint32) int {
	t.Helper()

	kb := dal.NewKeyBuilder()
	prefix := readstore.MetadataIndexPrefixV(kb, ledger, ns, key, version)
	iter, err := s.DB().NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: readstore.IncrementBytes(prefix),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}

	return n
}

// A retype landing mid-backfill must not refold the half-built keyspace.
// Events are permanent and stamped with the sequence of the log that caused
// them; re-encoding a value at its original sequence emits a retraction that
// loses the same-sequence tie to the standing ADD, forever. The reset
// therefore bumps the pending version, and the restarted replay fills a
// fresh keyspace while the abandoned one is left for the orphan reaper.
func TestRetypeDuringBackfill_RefillsAFreshVersion(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)
	b.accounts = make(map[string]struct{})

	const (
		ledger  = "test"
		metaKey = "k2"
		account = "acct:1"
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)
	canonical := indexes.Canonical(id)

	cfg := newLedgerIndexConfig()
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}

	// Creation backfill in flight: {cur:0, pend:1}, one log already folded
	// into v1 at its own sequence.
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 0,
		PendingVersion: 1,
		HighWater:      1,
	})
	b.backfillTasks = []*backfillTask{{
		ledger: ledger,
		index:  id,
		cursor: 42,
		bbKey:  backfillBBKey(ledger, id),
	}}

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(42)
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(account, metaKey, -1)))
	require.NoError(t, b.wb.Flush())

	v1Before := countEventsAt(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 1)
	require.Positive(t, v1Before, "premise: the half-built v1 holds the folded event")

	// The retype lands mid-backfill, inside the next fold batch.
	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.addSchemaRewriteTask(cfg, ledger, &commonpb.SetMetadataFieldTypeLog{
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        metaKey,
		Type:       commonpb.MetadataType_METADATA_TYPE_UINT32,
	}))

	state, ok := b.versionStateFor(ledger, canonical)
	require.True(t, ok)
	require.Equal(t, uint32(2), state.PendingVersion, "the restart targets a fresh keyspace")

	// The restarted replay re-folds the same log — same sequence, possibly a
	// different encoding — and every write must land in v2.
	b.wb.SetEventSequence(42)
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(account, metaKey, -1)))
	require.NoError(t, b.wb.Flush())

	assert.Equal(t, v1Before, countEventsAt(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 1),
		"the abandoned keyspace must not gain retractions it cannot honor — it is orphaned, not refolded")
	assert.Positive(t, countEventsAt(t, b.readStore, ledger, readstore.NamespaceAccount, metaKey, 2),
		"the refold lands in the fresh version")
}

// The transaction-metadata fast path relies on the same fresh-version
// invariant as the generic backfill reset. Exercise the actual production
// triggers independently: CreatedTransaction and RevertedTransaction first
// build v1, then the retype resets the task and their replay inserts into v2
// without retracting or reusing v1.
func TestRetypeDuringBackfill_NewTransactionMetadataReplaysIntoFreshVersion(t *testing.T) {
	t.Parallel()

	const (
		ledger  = "test"
		metaKey = "score"
	)

	tests := []struct {
		name  string
		txID  uint64
		index func(*Builder, *ledgerIndexConfig, *commonpb.MetadataValue) error
	}{
		{
			name: "CreatedTransaction",
			txID: 21,
			index: func(b *Builder, cfg *ledgerIndexConfig, value *commonpb.MetadataValue) error {
				return b.indexCreatedTransaction(b.kb, cfg, ledger, &commonpb.CreatedTransaction{
					Transaction: &commonpb.Transaction{
						Id:       21,
						Metadata: map[string]*commonpb.MetadataValue{metaKey: value},
					},
				}, nil)
			},
		},
		{
			name: "RevertedTransaction",
			txID: 22,
			index: func(b *Builder, cfg *ledgerIndexConfig, value *commonpb.MetadataValue) error {
				return b.indexRevertedTransaction(b.kb, cfg, ledger, &commonpb.RevertedTransaction{
					RevertTransaction: &commonpb.Transaction{
						Id:       22,
						Metadata: map[string]*commonpb.MetadataValue{metaKey: value},
					},
				}, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			b.seedBatchSchema(t)
			b.accounts = make(map[string]struct{})

			id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, metaKey)
			canonical := indexes.Canonical(id)
			cfg := newLedgerIndexConfig()
			cfg.byCanonical[canonical] = &commonpb.Index{Id: id}

			b.putVersionState(ledger, canonical, readstore.IndexVersionState{
				CurrentVersion: 0,
				PendingVersion: 1,
				HighWater:      1,
			})
			b.backfillTasks = []*backfillTask{{
				ledger: ledger,
				index:  id,
				cursor: 42,
				bbKey:  backfillBBKey(ledger, id),
			}}

			rawValue := commonpb.NewStringValue("030")
			oldEncoded := readstore.EncodeMetadataValue(nil, rawValue)
			newEncoded := readstore.EncodeMetadataValue(nil, commonpb.NewUintValue(30))
			entityID := readstore.EncodeTxID(make([]byte, 0, 8), tt.txID)
			v1Rmap := cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledger, tt.txID, metaKey, 1))
			v2Rmap := cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledger, tt.txID, metaKey, 2))

			// The first partial pass folds the transaction into v1.
			batch := b.readStore.NewBatch()
			b.initBatch(batch)
			b.wb.SetEventSequence(42)
			require.NoError(t, tt.index(b, cfg, rawValue))
			require.NoError(t, b.wb.Flush())

			requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, metaKey, 1, oldEncoded, entityID)
			assertReadStoreValue(t, b, v1Rmap, oldEncoded)

			// Retyping while the creation backfill is BUILDING bumps pending
			// to v2 and resets the replay cursor before the transaction log is
			// encountered again.
			batch = b.readStore.NewBatch()
			b.initBatch(batch)
			require.NoError(t, b.addSchemaRewriteTask(cfg, ledger, &commonpb.SetMetadataFieldTypeLog{
				TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				Key:        metaKey,
				Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
			}))

			state, ok := b.versionStateFor(ledger, canonical)
			require.True(t, ok)
			require.Equal(t, uint32(2), state.PendingVersion)
			require.Zero(t, b.backfillTasks[0].cursor)

			b.wb.SetEventSequence(42)
			require.NoError(t, tt.index(b, cfg, rawValue))
			require.NoError(t, b.wb.Flush())

			// v1 is abandoned intact; the replay emits one ADD plus its
			// existence/reverse-map writes in the fresh v2 keyspace.
			requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, metaKey, 1, oldEncoded, entityID)
			assertReadStoreValue(t, b, v1Rmap, oldEncoded)
			requireMetadataLive(t, b, ledger, readstore.NamespaceTransaction, metaKey, 2, newEncoded, entityID)
			assertReadStoreValue(t, b, v2Rmap, newEncoded)
			assert.Equal(t, 1, countEventsAt(t, b.readStore, ledger, readstore.NamespaceTransaction, metaKey, 2))
		})
	}
}
