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

// A field removal must leave NO per-replica version record behind. That
// absence is load-bearing: the query layer reads it as "removed" rather than
// "still building" (requireIndexReady), which is the difference between an
// honest rejection and telling the client to wait for a readiness that will
// never arrive. The rows go with it, so no reader can resolve a keyspace the
// schema no longer declares.
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

	stateBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(stateBatch, ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
	}))
	require.NoError(t, stateBatch.Commit())

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
	require.Zero(t, versionOf("after fold"), "no record: the query layer reads this as removed, not building")

	_, present, err := readstore.ReadIndexVersionStateFrom(b.readStore.DB(), ledger, canonical)
	require.NoError(t, err)
	require.False(t, present)
}
