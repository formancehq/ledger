package usagebuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

func TestBatchState_AddCounterAggregates(t *testing.T) {
	t.Parallel()

	s := newBatchState()

	s.addCounter("l1", usagestore.CounterPosting, 3)
	s.addCounter("l1", usagestore.CounterPosting, 5)
	s.addCounter("l1", usagestore.CounterRevert, 2)
	s.addCounter("l2", usagestore.CounterPosting, 7)

	assert.Equal(t, counterDelta(8), s.counters["l1"][usagestore.CounterPosting])
	assert.Equal(t, counterDelta(2), s.counters["l1"][usagestore.CounterRevert])
	assert.Equal(t, counterDelta(7), s.counters["l2"][usagestore.CounterPosting])
}

func TestBatchState_AddTemplateUsageMaxLastUsed(t *testing.T) {
	t.Parallel()

	s := newBatchState()

	early := &commonpb.Timestamp{Data: 100}
	mid := &commonpb.Timestamp{Data: 500}
	late := &commonpb.Timestamp{Data: 900}

	// Ordering shouldn't matter — the max of the three timestamps wins.
	s.addTemplateUsage("l1", "payout", mid)
	s.addTemplateUsage("l1", "payout", late)
	s.addTemplateUsage("l1", "payout", early)

	got := s.templates[templateKey{ledger: "l1", template: "payout"}]
	assert.Equal(t, uint64(3), got.count)
	assert.Equal(t, late, got.lastUsed)
}

func TestBatchState_AddTemplateUsageNilTimestamp(t *testing.T) {
	t.Parallel()

	s := newBatchState()

	// A nil timestamp bumps the counter but must not overwrite an already-seen
	// non-nil lastUsed.
	s.addTemplateUsage("l1", "payout", &commonpb.Timestamp{Data: 500})
	s.addTemplateUsage("l1", "payout", nil)

	got := s.templates[templateKey{ledger: "l1", template: "payout"}]
	assert.Equal(t, uint64(2), got.count)
	assert.Equal(t, uint64(500), got.lastUsed.GetData())
}

func TestBatchState_EmptyOnFreshState(t *testing.T) {
	t.Parallel()

	assert.True(t, newBatchState().empty())
}

func TestApplyDelta(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint64(15), applyDelta(10, 5))
	assert.Equal(t, uint64(5), applyDelta(10, -5))
	assert.Equal(t, uint64(0), applyDelta(10, -10))
	assert.Equal(t, uint64(0), applyDelta(3, -10), "underflow must clamp at zero")
	assert.Equal(t, uint64(7), applyDelta(7, 0))
}

func TestTimestampGreater(t *testing.T) {
	t.Parallel()

	a := &commonpb.Timestamp{Data: 100}
	b := &commonpb.Timestamp{Data: 200}

	assert.True(t, timestampGreater(b, a))
	assert.False(t, timestampGreater(a, b))
	assert.False(t, timestampGreater(a, a), "equal timestamps are not greater")
}

func TestMergeTemplateUsage_NilCurrent(t *testing.T) {
	t.Parallel()

	ts := &commonpb.Timestamp{Data: 500}

	got := mergeTemplateUsage(nil, templateDelta{count: 3, lastUsed: ts})
	assert.Equal(t, uint64(3), got.GetCount())
	assert.Equal(t, ts, got.GetLastUsed())
}

func TestMergeTemplateUsage_WithCurrent(t *testing.T) {
	t.Parallel()

	current := &commonpb.TemplateUsage{
		Count:    10,
		LastUsed: &commonpb.Timestamp{Data: 500},
	}

	// Newer batch timestamp wins.
	got := mergeTemplateUsage(current, templateDelta{count: 5, lastUsed: &commonpb.Timestamp{Data: 900}})
	assert.Equal(t, uint64(15), got.GetCount())
	assert.Equal(t, uint64(900), got.GetLastUsed().GetData())

	// Older batch timestamp is ignored.
	got = mergeTemplateUsage(current, templateDelta{count: 2, lastUsed: &commonpb.Timestamp{Data: 200}})
	assert.Equal(t, uint64(12), got.GetCount())
	assert.Equal(t, uint64(500), got.GetLastUsed().GetData(), "current lastUsed is later — keep it")

	// Nil batch timestamp: current lastUsed preserved.
	got = mergeTemplateUsage(current, templateDelta{count: 1, lastUsed: nil})
	assert.Equal(t, uint64(11), got.GetCount())
	assert.Equal(t, uint64(500), got.GetLastUsed().GetData())
}

// TestCommitBatch_SameBatchDeleteRecreateDoesNotResurrect guards the
// same-batch delete+recreate hazard: GetCounter / GetTemplateUsage read the
// committed DB, not the pending batch's DeleteRange, so without a zero
// baseline for deleted ledgers commitBatch would write old+delta on top of
// the DeleteRange and resurrect the old incarnation's counts.
func TestCommitBatch_SameBatchDeleteRecreateDoesNotResurrect(t *testing.T) {
	t.Parallel()

	us, err := usagestore.New(t.TempDir(), logging.NopZap(), usagestore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = us.Close() })

	b := &Builder{usageStore: us}

	// Seed the OLD incarnation: 100 postings and a template with count 7.
	seed := newBatchState()
	seed.addCounter("foo", usagestore.CounterPosting, 100)
	seed.addTemplateUsage("foo", "tpl", &commonpb.Timestamp{Data: 111})
	for range 6 {
		seed.addTemplateUsage("foo", "tpl", &commonpb.Timestamp{Data: 111})
	}
	require.NoError(t, b.commitBatch(seed, 1))

	// Sanity: the old incarnation is persisted.
	c, err := us.GetCounter("foo", usagestore.CounterPosting)
	require.NoError(t, err)
	require.Equal(t, uint64(100), c)

	// A single batch deletes "foo" and then recreates it with 5 new
	// postings + a single template invocation.
	batch := newBatchState()
	batch.markLedgerDeleted("foo")
	batch.addCounter("foo", usagestore.CounterPosting, 5)
	batch.addTemplateUsage("foo", "tpl", &commonpb.Timestamp{Data: 999})
	require.NoError(t, b.commitBatch(batch, 2))

	// The recycled ledger must reflect ONLY the post-recreate deltas, not
	// old+delta.
	c, err = us.GetCounter("foo", usagestore.CounterPosting)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), c, "counter must not resurrect the deleted incarnation's value")

	tpl, err := us.GetTemplateUsage("foo", "tpl")
	require.NoError(t, err)
	require.NotNil(t, tpl)
	assert.Equal(t, uint64(1), tpl.GetCount(), "template count must not resurrect the deleted incarnation")
	assert.Equal(t, uint64(999), tpl.GetLastUsed().GetData())
}
