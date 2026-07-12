package usagebuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func newBuilderWithUsageStore(t *testing.T) (*Builder, *usagestore.Store) {
	t.Helper()

	// Back the builder with a real (empty) primary store so the shared reset
	// path (resetProjection → RestoreGeneration) is exercised without a nil
	// dereference. The position-based rewind tests pass (cursor, head) directly,
	// so the empty store's audit head is never sampled.
	b, _, us := newBuilderWithStores(t)

	return b, us
}

// newBuilderWithStores wires a Builder over a real primary dal.Store and a real
// usagestore, so tests can exercise the restore-generation path (which reads
// pebbleStore.RestoreGeneration and samples the audit head).
func newBuilderWithStores(t *testing.T) (*Builder, *dal.Store, *usagestore.Store) {
	t.Helper()

	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("test")

	main, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = main.Close() })

	us, err := usagestore.New(t.TempDir(), logger, usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = us.Close() })

	return &Builder{
		pebbleStore: main,
		usageStore:  us,
		logger:      logger,
		batchSize:   DefaultBatchSize,
	}, main, us
}

// writeAuditEntry appends a minimal failed audit entry so the audit head
// advances without an AppliedProposal / items projection: the usagebuilder
// skips failed proposals (no state delta), which is all this test needs to
// drive the cursor forward past the generation-triggered reset.
func writeAuditEntry(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	entry := &auditpb.AuditEntry{
		Sequence:   seq,
		ProposalId: seq,
		Timestamp:  &commonpb.Timestamp{Data: seq * 1_000_000},
		Outcome:    &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{}},
		Ledgers:    []string{"l1"},
	}

	val, err := proto.Marshal(entry)
	require.NoError(t, err)

	batch := store.OpenWriteSession()
	kb := dal.NewKeyBuilder()
	require.NoError(t, batch.SetBytes(
		kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(seq).Build(),
		val,
	))
	require.NoError(t, batch.Commit())
}

// TestRewindIfCursorAhead_RuntimeRestore is the regression guard for the
// follower-sync corruption window: a primary-store restore (RestoreCheckpoint)
// drops the audit head below the persisted cursor WHILE the builder is running.
// tick() re-evaluates the cursor-ahead signature each pass and must rewind —
// wiping the stale rows and resetting the cursor to 0 — so the rolled-back gap
// is re-processed rather than skipped forever.
func TestRewindIfCursorAhead_RuntimeRestore(t *testing.T) {
	t.Parallel()

	b, us := newBuilderWithUsageStore(t)

	// Simulate a projection that had consumed up to audit seq 500: stale
	// counter + template rows plus a cursor at 500.
	batch := us.NewBatch()
	require.NoError(t, us.PutCounter(batch, "l1", usagestore.CounterPosting, 42))
	require.NoError(t, us.PutCounter(batch, "l1", usagestore.CounterVolume, 7))
	require.NoError(t, us.PutTemplateUsage(batch, "l1", "t1", &commonpb.TemplateUsage{Count: 9}))
	require.NoError(t, us.WriteProgress(batch, 500))
	require.NoError(t, batch.Commit())

	b.lastProcessedAuditSeq.Store(500)

	// Primary store was restored to head 120 (< cursor 500). The steady-state
	// (boot=false) path detects the direct cursor-ahead signature.
	rewound, err := b.rewindOnRollback(500, 120, false)
	require.NoError(t, err)
	require.True(t, rewound, "cursor 500 ahead of head 120 must trigger a rewind")

	// Projection wiped.
	c, err := us.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), c, "stale posting counter must be wiped")

	v, err := us.GetCounter("l1", usagestore.CounterVolume)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v, "stale volume counter must be wiped")

	tu, err := us.GetTemplateUsage("l1", "t1")
	require.NoError(t, err)
	assert.Nil(t, tu, "stale template row must be wiped")

	// Persisted cursor and the in-memory hint both reset to 0 so catch-up
	// re-processes the surviving audit chain from the start.
	seq, err := us.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "persisted cursor must rewind to 0")
	assert.Equal(t, uint64(0), b.lastProcessedAuditSeq.Load(), "in-memory cursor hint must rewind to 0")
}

// TestRewindIfCursorAhead_SteadyStateNoOp confirms the common case — cursor at
// or behind the audit head — leaves the projection and cursor untouched.
func TestRewindIfCursorAhead_SteadyStateNoOp(t *testing.T) {
	t.Parallel()

	b, us := newBuilderWithUsageStore(t)

	batch := us.NewBatch()
	require.NoError(t, us.PutCounter(batch, "l1", usagestore.CounterPosting, 42))
	require.NoError(t, us.WriteProgress(batch, 100))
	require.NoError(t, batch.Commit())
	b.lastProcessedAuditSeq.Store(100)

	// Head 300 is ahead of cursor 100 — normal steady state, no rewind. No
	// rebuildThreshold configured, so the gap heuristic is inert too.
	rewound, err := b.rewindOnRollback(100, 300, false)
	require.NoError(t, err)
	require.False(t, rewound)

	c, err := us.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), c, "counter must survive when cursor is not ahead")

	seq, err := us.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(100), seq, "cursor must be untouched when not ahead")

	// Cursor exactly at head is also steady state (not ahead).
	rewound, err = b.rewindOnRollback(300, 300, false)
	require.NoError(t, err)
	assert.False(t, rewound, "cursor == head is not ahead")
}

// TestRewindOnRollback_BootGapThreshold is the auditindexer-parity regression:
// after an in-place primary-store restore, the restored gap can re-grow past
// the stale cursor before boot samples the head, so cursorAheadOfHead never
// fires. The boot-only gap heuristic (head−cursor > rebuildThreshold) is the
// secondary net that still triggers the reset/replay.
func TestRewindOnRollback_BootGapThreshold(t *testing.T) {
	t.Parallel()

	b, us := newBuilderWithUsageStore(t)
	b.rebuildThreshold = 1000

	// Stale cursor at 500 with a leftover row; the restored-then-caught-up
	// head is 5000 — head > cursor (so cursorAheadOfHead is FALSE), but the
	// 4500-entry gap exceeds the threshold.
	batch := us.NewBatch()
	require.NoError(t, us.PutCounter(batch, "l1", usagestore.CounterVolume, 3))
	require.NoError(t, us.WriteProgress(batch, 500))
	require.NoError(t, batch.Commit())
	b.lastProcessedAuditSeq.Store(500)

	// Boot path (boot=true): the gap heuristic fires even though cursor<head.
	rewound, err := b.rewindOnRollback(500, 5000, true)
	require.NoError(t, err)
	require.True(t, rewound, "boot gap over threshold must trigger a rewind even when cursor is behind head")

	v, err := us.GetCounter("l1", usagestore.CounterVolume)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), v, "stale row must be wiped")

	seq, err := us.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), seq, "cursor must rewind to 0")

	// The SAME gap on the steady-state path (boot=false) must NOT rebuild —
	// a large head−cursor gap between ticks is normal ingest, not a rollback.
	b.lastProcessedAuditSeq.Store(500)
	rewound, err = b.rewindOnRollback(500, 5000, false)
	require.NoError(t, err)
	assert.False(t, rewound, "gap heuristic is boot-only; steady-state must not rebuild on a normal ingest burst")

	// A boot gap within the threshold is normal catch-up — no rewind.
	rewound, err = b.rewindOnRollback(500, 900, true)
	require.NoError(t, err)
	assert.False(t, rewound, "gap within threshold must not trigger a rewind")
}

// TestTickResetsOnRestoreGenerationChange is the regression for the catch-up
// race the cursor>head signal cannot catch: the primary store is restored
// beneath the usage cursor, then the audit head re-grows PAST the old cursor
// before tick() re-samples — so cursorAheadOfHead is false. The restore-
// generation change is the only remaining rollback signal, and tick() must
// reset the projection and rebuild from 0.
func TestTickResetsOnRestoreGenerationChange(t *testing.T) {
	t.Parallel()

	b, main, us := newBuilderWithStores(t)
	ctx := context.Background()

	// Seed the surviving audit chain (1..3) into the primary store and take a
	// checkpoint — this is the state a runtime restore will roll back to.
	for s := uint64(1); s <= 3; s++ {
		writeAuditEntry(t, main, s)
	}
	checkpointID, err := main.CreateSnapshot()
	require.NoError(t, err)

	// A projection that had already consumed up to audit seq 3, with stale rows.
	batch := us.NewBatch()
	require.NoError(t, us.PutCounter(batch, "l1", usagestore.CounterPosting, 42))
	require.NoError(t, us.WriteProgress(batch, 3))
	require.NoError(t, batch.Commit())

	// boot seeds the generation baseline; head (3) == cursor (3), so no rewind.
	require.NoError(t, b.boot(ctx))
	require.Equal(t, uint64(3), b.lastProcessedAuditSeq.Load())

	// Runtime rollback: a real RestoreCheckpoint bumps the store generation, and
	// the post-restore head re-grows to 5 — strictly ABOVE the stale cursor (3),
	// so cursorAheadOfHead is inert and only the generation change can fire.
	require.NoError(t, main.RestoreCheckpoint(checkpointID))
	for s := uint64(4); s <= 5; s++ {
		writeAuditEntry(t, main, s)
	}

	head, err := b.sampleAuditHead()
	require.NoError(t, err)
	require.Equal(t, uint64(5), head)
	require.False(t, cursorAheadOfHead(3, head), "head re-grew past cursor: position signal is inert")

	// tick() must reset on the generation change and re-drain the surviving
	// chain from 0, landing the cursor back at the head.
	require.NoError(t, b.tick(ctx))

	seq, err := us.ReadProgress()
	require.NoError(t, err)
	require.Equal(t, uint64(5), seq, "reset+rebuild must re-consume the full surviving chain")

	c, err := us.GetCounter("l1", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), c, "stale counter must be wiped by the reset")

	// A second tick with no further restore must NOT reset (generation resync).
	require.NoError(t, b.tick(ctx))
	seq, err = us.ReadProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(5), seq, "no reset without a new restore")
}
