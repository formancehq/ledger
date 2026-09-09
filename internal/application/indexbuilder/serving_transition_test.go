package indexbuilder

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A promotion is only safe to serve once it is on disk: the read store has
// no WAL, so a kill between the commit and Pebble's next flush would reopen
// the node at the pre-promotion state while queries had already been
// answered under the promoted binding. These tests reopen what the read store
// holds on disk right after each promotion path — the image a hard kill would
// leave — and require the promotion to be there.

const servingTestLedger = "ledger1"

// flushGate lets a test fail the promotion flush on demand. The store is built
// with it, so nothing mutates the store after construction.
type flushGate struct{ failing atomic.Bool }

func (g *flushGate) wrap(flush func() error) error {
	if g.failing.Load() {
		return errors.New("disk on fire")
	}

	return flush()
}

func newKillableTestBuilder(t *testing.T) (*Builder, *flushGate) {
	t.Helper()

	gate := &flushGate{}

	store, err := readstore.New(t.TempDir(), noopLogger{}, readstore.DefaultConfig(), readstore.WithPromotionFlushForTest(gate.wrap))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	fsm, err := dal.NewStore(t.TempDir(), noopLogger{}, metricnoop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsm.Close() })

	return &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   store,
		pebbleStore: fsm,
		attrs:       attributes.New(),
		kb:          dal.NewKeyBuilder(),
		wb:          readstore.NewWriteBatch(),
		logger:      noopLogger{},
	}, gate
}

// reopenReadStoreImage opens what the read store holds on disk now. A Pebble
// checkpoint is a consistent copy of the flushed SSTables and, with the WAL
// disabled, carries no memtable — what a hard kill keeps. It must go through
// pebble.DB.Checkpoint directly: readstore.Store.CreateCheckpoint flushes
// first, which would put the memtable on disk and hide a missing promotion
// flush from these tests.
func reopenReadStoreImage(t *testing.T, rs *readstore.Store) *readstore.Store {
	t.Helper()

	image := t.TempDir()
	require.NoError(t, rs.DB().Checkpoint(filepath.Join(image, "readindex")))

	reopened, err := readstore.New(image, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	return reopened
}

// The mark is up when the commit runs and stays up after it, so no reader can
// observe a committed promotion that is not refused; a failed commit withdraws
// the mark, since nothing reached the store.
func TestBuilder_CommitPromotion_MarksBeforeTheCommit(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)
	st := b.readStore

	const canonical = "meta:account:score"

	committed := false
	err := b.commitPromotion(servingTestLedger, canonical, func() error {
		assert.True(t, st.PromotionInFlight(servingTestLedger, canonical), "the mark is up when the commit runs")
		committed = true

		return nil
	})
	require.NoError(t, err)
	require.True(t, committed)
	assert.True(t, st.PromotionInFlight(servingTestLedger, canonical), "a committed promotion stays refused until its flush")

	require.NoError(t, st.FlushPromotions())
	assert.False(t, st.PromotionInFlight(servingTestLedger, canonical))

	err = b.commitPromotion(servingTestLedger, canonical, func() error {
		assert.True(t, st.PromotionInFlight(servingTestLedger, canonical), "the mark is up when the commit runs")

		return errors.New("disk on fire")
	})
	require.Error(t, err)
	assert.False(t, st.PromotionInFlight(servingTestLedger, canonical), "a failed commit withdraws the mark")
}

// seedFlushedVersionState persists state as the flushed baseline a kill
// would rewind to.
func seedFlushedVersionState(t *testing.T, b *Builder, canonical string, state readstore.IndexVersionState) {
	t.Helper()

	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(batch, servingTestLedger, canonical, state))
	require.NoError(t, batch.Commit())
	require.NoError(t, b.readStore.DB().Flush())
	b.putVersionState(servingTestLedger, canonical, state)
}

func TestCompleteBackfill_PromotionSurvivesAKill(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)

	seedFlushedVersionState(t, b, canonical, readstore.IndexVersionState{
		PendingVersion:      1,
		HighWater:           1,
		PendingType:         commonpb.MetadataType_METADATA_TYPE_INT64,
		PendingTypeDeclared: true,
	})

	require.NoError(t, b.completeBackfill(&backfillTask{ledger: servingTestLedger, index: id}))

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical), "the flush completed, so nothing is left in flight")

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(1), state.CurrentVersion, "a kill right after completeBackfill must keep the promotion")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, state.CurrentType)
}

func TestScanCompleteSwitch_PromotionSurvivesAKill(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.scanComplete = true

	done, err := b.tryCommitScanCompleteSwitch(task, b.kb, readstore.NamespaceAccount, canonical, 1, 2)
	require.NoError(t, err)
	require.True(t, done)

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical))

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion, "a kill right after the deferred switch must keep the promotion")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT8, state.CurrentType)
	assert.Zero(t, state.PendingVersion)
}

// seedRetypeInFlight stands the index at "v1 served, v2 pending" with the
// rewrite's alignment gate already satisfied, so either switch path fires on
// the next call.
func seedRetypeInFlight(t *testing.T, b *Builder, canonical string) *schemaRewriteTask {
	t.Helper()

	seedFlushedVersionState(t, b, canonical, readstore.IndexVersionState{
		CurrentVersion:      1,
		PendingVersion:      2,
		HighWater:           2,
		CurrentType:         commonpb.MetadataType_METADATA_TYPE_INT32,
		CurrentTypeDeclared: true,
		PendingType:         commonpb.MetadataType_METADATA_TYPE_INT8,
		PendingTypeDeclared: true,
	})
	seedRewriteSequence(t, b, 5)

	return &schemaRewriteTask{
		ledger:             servingTestLedger,
		targetType:         commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		key:                "grade",
		toType:             commonpb.MetadataType_METADATA_TYPE_INT8,
		bbKey:              schemaRewriteBBKey(servingTestLedger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade"),
		requiredIndexedSeq: 5,
	}
}

// The immediate switch inside processSchemaRewrite shares the flush with the
// deferred one; this pins the scan-and-switch path end to end. An empty
// reverse map makes the scan exhaust at once, so the switch fires in the
// scan's own batch.
func TestSchemaRewriteImmediateSwitch_PromotionSurvivesAKill(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.requiredIndexedSeq = 0

	stop := make(chan struct{})
	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, done)

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical))

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion, "a kill right after the immediate switch must keep the promotion")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT8, state.CurrentType)
}

// A flush that fails after the promotion committed must not lift the mark:
// the promoted state is in the store, so the only thing standing between a
// reader and a non-durable binding is the refusal. The promotion itself
// succeeds (state committed and cached) and the next tick's retry makes it
// durable and servable.
func requirePromotionHeldUntilRetry(t *testing.T, b *Builder, gate *flushGate, canonical string) {
	t.Helper()

	assert.True(t, b.readStore.PromotionInFlight(servingTestLedger, canonical), "a failed flush must keep the mark")

	live, present, err := b.readStore.ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), live.CurrentVersion, "the promotion is committed")

	current, pending := b.versionFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(2), current, "and published to the builder's cache")
	assert.Zero(t, pending)

	gate.failing.Store(false)
	b.retryPromotionFlush()
	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical), "the retry lifts the mark")

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion)
}

func TestScanCompleteSwitch_FlushFailureKeepsThePromotionRefused(t *testing.T) {
	t.Parallel()

	b, gate := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.scanComplete = true

	gate.failing.Store(true)

	done, err := b.tryCommitScanCompleteSwitch(task, b.kb, readstore.NamespaceAccount, canonical, 1, 2)
	require.NoError(t, err, "the switch committed; a failed flush is not a failed switch")
	require.True(t, done)

	requirePromotionHeldUntilRetry(t, b, gate, canonical)
}

func TestSchemaRewriteImmediateSwitch_FlushFailureKeepsThePromotionRefused(t *testing.T) {
	t.Parallel()

	b, gate := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.requiredIndexedSeq = 0

	gate.failing.Store(true)

	stop := make(chan struct{})
	done, err := b.processSchemaRewrite(task, 10, stop, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, done)

	requirePromotionHeldUntilRetry(t, b, gate, canonical)
}

func TestCompleteBackfill_FlushFailureKeepsThePromotionRefused(t *testing.T) {
	t.Parallel()

	b, gate := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)

	seedFlushedVersionState(t, b, canonical, readstore.IndexVersionState{
		PendingVersion:      2,
		HighWater:           2,
		PendingType:         commonpb.MetadataType_METADATA_TYPE_INT8,
		PendingTypeDeclared: true,
	})

	gate.failing.Store(true)

	require.NoError(t, b.completeBackfill(&backfillTask{ledger: servingTestLedger, index: id}))

	requirePromotionHeldUntilRetry(t, b, gate, canonical)
}
