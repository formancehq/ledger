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
