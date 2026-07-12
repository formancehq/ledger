package usagebuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func newBuilderWithUsageStore(t *testing.T) (*Builder, *usagestore.Store) {
	t.Helper()

	us, err := usagestore.New(t.TempDir(), logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = us.Close() })

	return &Builder{usageStore: us, logger: logging.NopZap()}, us
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
