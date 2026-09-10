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
	err := b.commitPromotion(servingTestLedger, canonical, 1, func() error {
		assert.True(t, st.PromotionInFlight(servingTestLedger, canonical, 1), "the mark is up when the commit runs")
		committed = true

		return nil
	})
	require.NoError(t, err)
	require.True(t, committed)
	assert.True(t, st.PromotionInFlight(servingTestLedger, canonical, 1), "a committed promotion stays unserved until its flush")

	require.NoError(t, st.FlushPromotions())
	assert.False(t, st.PromotionInFlight(servingTestLedger, canonical, 1))

	err = b.commitPromotion(servingTestLedger, canonical, 1, func() error {
		assert.True(t, st.PromotionInFlight(servingTestLedger, canonical, 1), "the mark is up when the commit runs")

		return errors.New("disk on fire")
	})
	require.Error(t, err)
	assert.False(t, st.PromotionInFlight(servingTestLedger, canonical, 1), "a failed commit withdraws the mark")
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

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical, 1), "the flush completed, so nothing is left in flight")

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

	done, err := b.tryCommitScanCompleteSwitch(task, canonical, 1, 2)
	require.NoError(t, err)
	require.True(t, done)

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical, 2))

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion, "a kill right after the deferred switch must keep the promotion")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT8, state.CurrentType)
	assert.Zero(t, state.PendingVersion)
	assert.Equal(t, uint32(1), state.PreviousVersion, "the retirement that followed is not flushed, so the image still retains v1")
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

	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical, 2))

	state, present, err := reopenReadStoreImage(t, b.readStore).ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), state.CurrentVersion, "a kill right after the immediate switch must keep the promotion")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT8, state.CurrentType)
	assert.Equal(t, uint32(1), state.PreviousVersion, "the retirement that followed is not flushed, so the image still retains v1")
}

// A flush that fails after the promotion committed must not lift the mark:
// the promoted state is in the store, so the only thing standing between a
// reader and a non-durable binding is the mark. The promotion itself
// succeeds (state committed and cached) and the next tick's retry makes it
// durable and servable.
func requirePromotionHeldUntilRetry(t *testing.T, b *Builder, gate *flushGate, canonical string) {
	t.Helper()

	assert.True(t, b.readStore.PromotionInFlight(servingTestLedger, canonical, 2), "a failed flush must keep the mark")

	live, present, err := b.readStore.ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	require.True(t, present)
	assert.Equal(t, uint32(2), live.CurrentVersion, "the promotion is committed")

	current, pending := b.versionFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(2), current, "and published to the builder's cache")
	assert.Zero(t, pending)

	gate.failing.Store(false)
	b.flushPromotions()
	assert.False(t, b.readStore.PromotionInFlight(servingTestLedger, canonical, 2), "the retry lifts the mark")

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

	done, err := b.tryCommitScanCompleteSwitch(task, canonical, 1, 2)
	require.NoError(t, err, "the switch committed; a failed flush is not a failed switch")
	require.True(t, done)

	resolved := resolveAt(t, b, canonical, 5)
	assert.Equal(t, uint32(1), resolved.Version, "while the promotion is unflushed readers are served from the retained version")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT32, resolved.Type, "under its own binding")

	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(1), state.PreviousVersion, "the retained version is not retired while the promotion is unflushed")

	requirePromotionHeldUntilRetry(t, b, gate, canonical)

	assert.Equal(t, uint32(2), resolveAt(t, b, canonical, 5).Version)

	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion, "the retry also retired the replaced version")
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

func resolveAt(t *testing.T, b *Builder, canonical string, pin uint64) readstore.ResolvedIndexVersion {
	t.Helper()

	resolved, primed, err := b.readStore.PinnedVersionResolver(b.readStore.DB(), servingTestLedger, pin)(canonical)
	require.NoError(t, err)
	require.True(t, primed)

	return resolved
}

// v1Row writes one row into v1's forward keyspace for the grade index and
// returns a probe for it.
func v1Row(t *testing.T, b *Builder) func() bool {
	t.Helper()

	key := append(readstore.MetadataIndexPrefixV(dal.NewKeyBuilder(), servingTestLedger, readstore.NamespaceAccount, "grade", 1), 'x')

	batch := b.readStore.NewBatch()
	require.NoError(t, batch.SetBytes(key, []byte{1}))
	require.NoError(t, batch.Commit())

	return func() bool {
		exists, err := b.readstoreKeyExists(key)
		require.NoError(t, err)

		return exists
	}
}

// The switch retains v1 — its rows and binding — while a live read is pinned
// below v2's activation, serving that read from v1; once the read releases,
// the next pass retires v1 in one batch.
func TestSchemaRewriteSwitch_RetainsThenRetiresTheReplacedVersion(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.scanComplete = true
	rowPresent := v1Row(t, b)

	lease := b.readStore.Leases().Reserve(2)

	done, err := b.tryCommitScanCompleteSwitch(task, canonical, 1, 2)
	require.NoError(t, err)
	require.True(t, done)

	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(2), state.CurrentVersion)
	assert.Equal(t, uint32(1), state.PreviousVersion)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT32, state.PreviousType)
	assert.Equal(t, uint64(5), state.PreviousValidThrough, "the fold cursor at the switch")
	assert.True(t, rowPresent(), "v1's keyspace survives the switch: a lease reserved below the activation holds it")

	assert.Equal(t, uint32(1), resolveAt(t, b, canonical, 4).Version, "a pin below v2's activation is served from v1")
	assert.Equal(t, uint32(2), resolveAt(t, b, canonical, 5).Version)

	b.flushPromotions()
	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(1), state.PreviousVersion, "still held")

	unrelated := b.readStore.Leases().Reserve(7)
	defer unrelated.Release()

	lease.Release()
	b.flushPromotions()

	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion, "retired: the remaining lease is at or past the activation and is served v2")
	assert.False(t, rowPresent(), "and its keyspace purged in the same batch")

	live, _, err := b.readStore.ReadIndexVersionState(servingTestLedger, canonical)
	require.NoError(t, err)
	assert.Zero(t, live.PreviousVersion, "the retirement is committed")
	assert.Zero(t, resolveAt(t, b, canonical, 4).Version, "nothing left to serve a pin below the activation")
}

// A second promotion of the same index waits while the version an earlier one
// replaced is still retained, and fires once it is retired.
func TestSchemaRewriteSwitch_DefersWhileAnEarlierVersionIsRetained(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	task := seedRetypeInFlight(t, b, canonical)
	task.scanComplete = true

	lease := b.readStore.Leases().Reserve(2)

	done, err := b.tryCommitScanCompleteSwitch(task, canonical, 1, 2)
	require.NoError(t, err)
	require.True(t, done)
	assert.True(t, b.promotionBlocked(servingTestLedger, canonical))

	retained, _ := b.versionStateFor(servingTestLedger, canonical)
	retained.PendingVersion, retained.HighWater = 3, 3
	retained.PendingType, retained.PendingTypeDeclared = commonpb.MetadataType_METADATA_TYPE_INT16, true
	b.putVersionState(servingTestLedger, canonical, retained)

	second := *task
	second.toType = commonpb.MetadataType_METADATA_TYPE_INT16

	done, err = b.tryCommitScanCompleteSwitch(&second, canonical, 2, 3)
	require.NoError(t, err)
	assert.False(t, done, "deferred: v1 is still retained")

	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(2), state.CurrentVersion, "the switch did not fire")
	assert.Equal(t, uint32(1), state.PreviousVersion)

	lease.Release()
	b.flushPromotions()
	assert.False(t, b.promotionBlocked(servingTestLedger, canonical), "the wake's pass retires v1 once the lease is gone")

	// The second switch activates at 9; a read between v2's activation (5)
	// and that holds v2 retained.
	seedRewriteSequence(t, b, 9)
	second.requiredIndexedSeq = 9
	lease = b.readStore.Leases().Reserve(6)
	defer lease.Release()

	done, err = b.tryCommitScanCompleteSwitch(&second, canonical, 2, 3)
	require.NoError(t, err)
	assert.True(t, done)

	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(3), state.CurrentVersion)
	assert.Equal(t, uint32(2), state.PreviousVersion, "at most one version is retained")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT8, state.PreviousType)
}

// A retained version found at boot is live to the orphan sweep and retired by
// the first retirement pass, registry entry or not.
func TestRetirePrevious_RetiresARetainedVersionFoundAtBoot(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)
	b.getOrCreateLedgerConfig(servingTestLedger).byCanonical[canonical] = &commonpb.Index{Id: id, Ledger: servingTestLedger}

	seedFlushedVersionState(t, b, canonical, readstore.IndexVersionState{
		CurrentVersion:       3,
		HighWater:            3,
		ActivationSequence:   5,
		PreviousVersion:      1,
		PreviousValidThrough: 5,
	})
	rowPresent := v1Row(t, b)

	require.NoError(t, b.purgeOrphanVersions())
	assert.True(t, rowPresent(), "a retained version is live to the orphan sweep")

	delete(b.getOrCreateLedgerConfig(servingTestLedger).byCanonical, canonical)
	b.retirePrevious()

	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion)
	assert.False(t, rowPresent())
}

// Only a lease that could still be served the retained version holds its
// retirement: one in [PreviousActivation, Activation). Leases are not scoped
// to a ledger, so that is any read in the bucket.
func TestRetirePrevious_LeaseGateIsTheServableWindow(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)

	retained := readstore.IndexVersionState{
		CurrentVersion:             2,
		HighWater:                  2,
		ActivationSequence:         10,
		PreviousVersion:            1,
		PreviousActivationSequence: 4,
		PreviousValidThrough:       10,
	}
	seedFlushedVersionState(t, b, canonical, retained)

	below := b.readStore.Leases().Reserve(3)
	atOrAbove := b.readStore.Leases().Reserve(10)
	b.retirePrevious()
	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion, "a lease below the retained version's activation, or at the current one's, cannot be served v1")
	below.Release()
	atOrAbove.Release()

	seedFlushedVersionState(t, b, canonical, retained)
	inWindow := b.readStore.Leases().Reserve(4)
	b.retirePrevious()
	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(1), state.PreviousVersion, "a lease at the retained version's activation holds it")
	inWindow.Release()

	b.retirePrevious()
	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion)
}

// A retype landing while a replaced version is retained keeps it retained:
// the bump touches only the pending slot, so the keyspace stays reachable
// until retirement.
func TestBumpPendingVersion_KeepsTheRetainedVersion(t *testing.T) {
	t.Parallel()

	b, _ := newKillableTestBuilder(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "grade")
	canonical := indexes.Canonical(id)

	retained := readstore.IndexVersionState{
		CurrentVersion:             2,
		HighWater:                  2,
		ActivationSequence:         5,
		CurrentType:                commonpb.MetadataType_METADATA_TYPE_INT8,
		CurrentTypeDeclared:        true,
		PreviousVersion:            1,
		PreviousType:               commonpb.MetadataType_METADATA_TYPE_INT32,
		PreviousTypeDeclared:       true,
		PreviousActivationSequence: 3,
		PreviousValidThrough:       5,
	}
	seedFlushedVersionState(t, b, canonical, retained)

	b.initBatch(b.readStore.NewBatch())
	require.NoError(t, b.bumpPendingVersion(servingTestLedger, id, commonpb.MetadataType_METADATA_TYPE_INT16))
	require.NoError(t, b.wb.Flush())

	state, _ := b.versionStateFor(servingTestLedger, canonical)
	assert.Equal(t, uint32(3), state.PendingVersion)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT16, state.PendingType)
	assert.Equal(t, uint32(1), state.PreviousVersion, "the retained version survives the bump")
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT32, state.PreviousType)
	assert.Equal(t, uint64(3), state.PreviousActivationSequence)
	assert.Equal(t, uint64(5), state.PreviousValidThrough)

	b.retirePrevious()

	state, _ = b.versionStateFor(servingTestLedger, canonical)
	assert.Zero(t, state.PreviousVersion, "and is retired once its gate opens")
	assert.Equal(t, uint32(3), state.PendingVersion, "retirement leaves the pending slot alone")
}
