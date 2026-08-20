package indexbuilder

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A field removal must leave the per-replica version record reading as
// REMOVED at the query layer — never as "still building" (requireIndexReady),
// which is the difference between an honest rejection and telling the client
// to wait for a readiness that will never arrive. The record itself survives
// as a tombstone {cur:0, pend:0, HighWater}: versions are single-use, so the
// high-water mark must outlive the index (IndexVersionState.HighWater). The
// rows still go with it, so no reader can resolve a keyspace the schema no
// longer declares.
func TestFieldRemoval_LeavesNoVersionRecordOrRows(t *testing.T) {
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
	kb := dal.NewKeyBuilder()

	cfg := newLedgerIndexConfig()
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}

	// The index is live with one row.
	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(1)
	require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(account, metaKey, 42)))
	require.NoError(t, b.wb.Flush())

	// Persisted AND cached, as production keeps them: boot loads every state
	// into the in-memory map, and the fold paths write both together.
	liveState := readstore.IndexVersionState{CurrentVersion: 1, HighWater: 1}

	stateBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(stateBatch, ledger, canonical, liveState))
	require.NoError(t, stateBatch.Commit())
	b.putVersionState(ledger, canonical, liveState)

	countRows := func(label string) int {
		prefix := readstore.MetadataIndexFieldPrefix(kb, ledger, readstore.NamespaceAccount, metaKey)
		iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: readstore.IncrementBytes(prefix),
		})
		require.NoError(t, err)

		defer func() { _ = iter.Close() }()

		n := 0
		for iter.First(); iter.Valid(); iter.Next() {
			n++
		}

		t.Logf("%s: rows=%d", label, n)

		return n
	}

	versionOf := func(label string) uint32 {
		st, ok, err := readstore.ReadIndexVersionStateFrom(b.readStore.DB(), ledger, canonical)
		require.NoError(t, err)
		t.Logf("%s: versionStatePresent=%v current=%d", label, ok, st.CurrentVersion)

		return st.CurrentVersion
	}

	require.Positive(t, countRows("before removal"))
	require.Equal(t, uint32(1), versionOf("before removal"))

	// The removal folds: this is the handler the builder runs.
	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	require.NoError(t, b.handleRemovedMetadataFieldType(b.kb, cfg, ledger, &commonpb.RemovedMetadataFieldTypeLog{
		TargetType:   commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:          metaKey,
		DroppedIndex: id,
	}))

	// Mid-fold: the batch has NOT committed yet. Whatever a reader sees now is
	// what the direct (unbatched) writes inside the handler already did.
	countRows("mid-fold, batch uncommitted")
	versionOf("mid-fold, batch uncommitted")

	require.NoError(t, b.wb.Flush())

	require.Zero(t, countRows("after fold"), "the field's rows must not outlive its schema declaration")
	require.Zero(t, versionOf("after fold"), "no servable version: the query layer reads this as removed, not building")

	// The record survives as a tombstone carrying the high-water version…
	st, present, err := readstore.ReadIndexVersionStateFrom(b.readStore.DB(), ledger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	require.True(t, st.Tombstoned())
	require.Equal(t, uint32(1), st.HighWater, "the removed incarnation's version is retired forever")

	// …which queries must read exactly like an absent record: removed.
	_, primed, err := readstore.SnapshotVersionResolver(b.readStore.DB(), ledger)(canonical)
	require.NoError(t, err)
	require.False(t, primed, "a tombstone must resolve as removed, never as building")
}
