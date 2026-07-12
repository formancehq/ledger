package indexbuilder

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// countReadStoreKeys returns the number of read-store keys under prefix.
func countReadStoreKeys(t *testing.T, rs *readstore.Store, prefix []byte) int {
	t.Helper()

	iter, err := rs.DB().NewIter(&pebble.IterOptions{
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

// TestResetIndexes_WipesBuilderKeyspaceButNotAuditIndex pins the reset boundary:
// ResetIndexes must wipe every index-builder-owned keyspace (ledger-scoped index
// rows + the builder's internal cursors/version state) while leaving the
// auditindexer-owned keyspace (audit index rows + audit cursor) untouched — the
// audit indexer has its own independent rollback detector.
func TestResetIndexes_WipesBuilderKeyspaceButNotAuditIndex(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	rs := b.readStore
	kb := dal.NewKeyBuilder()

	// Seed builder-owned rows: a ledger-log index row, a backfill cursor, an
	// index-version state entry, and the log/applied-proposal progress cursors.
	batch := rs.NewBatch()
	require.NoError(t, rs.WriteProgress(batch, 500))
	require.NoError(t, rs.WriteAppliedProposalProgress(batch, 400))
	require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, "l1", 10), []byte{1}))
	require.NoError(t, rs.WriteBackfillProgress(batch, []byte("l1-key"), 99))
	require.NoError(t, rs.WriteIndexVersionState(batch, "l1", "canon", readstore.IndexVersionState{CurrentVersion: 1}))
	require.NoError(t, batch.Commit())

	// Seed auditindexer-owned rows: an audit index entry + the audit cursor.
	// These must survive the reset.
	auditBatch := rs.NewBatch()
	require.NoError(t, auditBatch.SetBytes(readstore.AuditIndexByteKey(kb, readstore.AuditFieldOutcome, 1, 7), nil))
	require.NoError(t, rs.WriteAuditProgress(auditBatch, 7))
	require.NoError(t, auditBatch.Commit())

	require.NoError(t, b.readStore.ResetIndexes())

	// Builder-owned keyspace fully wiped.
	assert.Equal(t, 0, countReadStoreKeys(t, rs, readstore.LedgerLogPrefix(kb, "l1")), "ledger log index rows must be wiped")
	assert.Equal(t, 0, countReadStoreKeys(t, rs, readstore.BackfillKeyPrefix()), "backfill cursors must be wiped")
	assert.Equal(t, 0, countReadStoreKeys(t, rs, readstore.IndexVersionStatePrefix()), "index version state must be wiped")

	cursor, err := rs.LastIndexedSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), cursor, "log progress cursor must be wiped to 0")

	ap, err := rs.ReadAppliedProposalProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ap, "applied-proposal cursor must be wiped to 0")

	// Auditindexer-owned keyspace untouched.
	assert.Equal(t, 1, countReadStoreKeys(t, rs, readstore.AuditIndexPrefix()), "audit index rows must survive the builder reset")

	auditCursor, err := rs.ReadAuditProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(7), auditCursor, "audit cursor must survive the builder reset")
}

// TestShouldRebuildOnBoot covers the boot rollback guard's decision table.
func TestShouldRebuildOnBoot(t *testing.T) {
	t.Parallel()

	b := &Builder{}

	// cursorAheadOfHead: the direct rollback signature.
	assert.True(t, b.shouldRebuildOnBoot(500, 120), "cursor ahead of head must rebuild")
	assert.False(t, b.shouldRebuildOnBoot(100, 300), "cursor behind head is normal catch-up")
	assert.False(t, b.shouldRebuildOnBoot(300, 300), "cursor == head is not ahead")
	assert.False(t, b.shouldRebuildOnBoot(0, 300), "fresh cursor (0) is normal catch-up, not a rollback")

	// Gap threshold (boot-only net for the catch-up race).
	b.rebuildThreshold = 1000
	assert.True(t, b.shouldRebuildOnBoot(500, 5000), "gap over threshold must rebuild even when cursor is behind head")
	assert.False(t, b.shouldRebuildOnBoot(500, 900), "gap within threshold is normal catch-up")

	// Threshold disabled (0): only the cursor-ahead signature remains.
	b.rebuildThreshold = 0
	assert.False(t, b.shouldRebuildOnBoot(500, 5000), "gap net is inert when threshold is 0")
}

// TestBootInit_ResetsOnPrimaryStoreRollback exercises the boot path end to end:
// a read index that consumed up to log seq 3, then a real RestoreCheckpoint
// rolls the primary log head back to 1 (below the cursor). bootInit must detect
// the cursor-ahead signature, wipe the read index, and return cursor 0 so the
// surviving chain is re-indexed from the start.
func TestBootInit_ResetsOnPrimaryStoreRollback(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	ctx := context.Background()

	// Surviving primary-store chain [1] + a checkpoint to roll back to.
	writeLogToFSM(t, b, makeCreatedTxLog(1, "l1", 100, []*commonpb.Posting{
		{Source: "a", Destination: "b", Asset: "USD/2"},
	}))
	checkpointID, err := b.pebbleStore.CreateSnapshot()
	require.NoError(t, err)

	// Grow the head to 3, then roll back to the checkpoint (head returns to 1).
	writeLogToFSM(t, b, makeCreatedTxLog(2, "l1", 101, []*commonpb.Posting{{Source: "b", Destination: "c", Asset: "USD/2"}}))
	writeLogToFSM(t, b, makeCreatedTxLog(3, "l1", 102, []*commonpb.Posting{{Source: "c", Destination: "d", Asset: "USD/2"}}))

	// Read index had consumed up to 3, with a stale ledger-log row for logID 102.
	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(batch, 3))
	require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, "l1", 102), []byte{1}))
	require.NoError(t, batch.Commit())

	require.NoError(t, b.pebbleStore.RestoreCheckpoint(checkpointID))

	head, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(1), head, "primary head rolled back below the read cursor")

	cursor, pebbleLast, err := b.bootInit(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), pebbleLast)
	assert.Equal(t, uint64(0), cursor, "boot must reset the cursor to 0 on rollback")

	// Read index cursor and the stale rows are wiped.
	persisted, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), persisted, "persisted cursor must be wiped")
	assert.Equal(t, 0, countReadStoreKeys(t, b.readStore, readstore.LedgerLogPrefix(kb, "l1")), "stale ledger-log rows must be wiped")
}

// TestMaybeResetOnRestore_CatchUpRace is the core regression: it reproduces the
// exact race the position-only cursorAheadOfHead guard cannot catch. The read
// index advances to log seq 3; a RestoreCheckpoint rolls the primary head back
// below the cursor; then the head RE-GROWS past the old cursor before the loop
// re-samples — so cursorAheadOfHead(cursor, head) is FALSE. The restore-
// generation change is the only surviving rollback signal, and the steady-state
// gate must force a reset+rebuild. Mirrors usagebuilder.TestTickResetsOnRestoreGenerationChange.
func TestMaybeResetOnRestore_CatchUpRace(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()
	ctx := context.Background()

	// Surviving chain [1..3] + a checkpoint at head 3 (the rollback target).
	for s := uint64(1); s <= 3; s++ {
		writeLogToFSM(t, b, makeCreatedTxLog(s, "l1", 100+s, []*commonpb.Posting{
			{Source: "a", Destination: "b", Asset: "USD/2"},
		}))
	}
	checkpointID, err := b.pebbleStore.CreateSnapshot()
	require.NoError(t, err)

	// Read index had consumed up to 3, with a stale ledger-log row.
	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteProgress(batch, 3))
	require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, "l1", 999), []byte{1}))
	require.NoError(t, batch.Commit())

	// boot seeds the generation baseline (head 3 == cursor 3, so no boot rewind).
	cursor, _, err := b.bootInit(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)

	// Runtime rollback: RestoreCheckpoint bumps the generation, then the head
	// RE-GROWS to 5 — strictly ABOVE the stale cursor (3), so the position
	// signal is inert and only the generation change can fire.
	require.NoError(t, b.pebbleStore.RestoreCheckpoint(checkpointID))
	for s := uint64(4); s <= 5; s++ {
		writeLogToFSM(t, b, makeCreatedTxLog(s, "l1", 100+s, []*commonpb.Posting{
			{Source: "a", Destination: "b", Asset: "USD/2"},
		}))
	}

	head, err := query.ReadLastSequence(mustReadHandle(t, b))
	require.NoError(t, err)
	require.Equal(t, uint64(5), head)
	require.False(t, cursorAheadOfHead(cursor, head), "head re-grew past cursor: the position guard is inert")

	// The position-only boot guard would stay inert on this (cursor, head)...
	require.False(t, b.shouldRebuildOnBoot(cursor, head), "position guard must NOT fire — this is the race it misses")

	// ...but the generation gate must force a reset+rebuild.
	newCursor, reset, err := b.maybeResetOnRestore(ctx, cursor)
	require.NoError(t, err)
	require.True(t, reset, "generation change must force a reset")
	assert.Equal(t, uint64(0), newCursor, "reset rewinds the cursor to 0")

	persisted, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), persisted, "persisted cursor must rewind to 0")
	assert.Equal(t, 0, countReadStoreKeys(t, b.readStore, readstore.LedgerLogPrefix(kb, "l1")), "stale ledger-log rows must be wiped by the reset")

	// A second call with no further restore must NOT reset (generation resync).
	_, reset, err = b.maybeResetOnRestore(ctx, 0)
	require.NoError(t, err)
	assert.False(t, reset, "no reset without a new restore (restoreGen was re-synced)")
}
