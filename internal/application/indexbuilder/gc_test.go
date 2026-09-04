package indexbuilder

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestRunEventGC_StopsScanningAfterCompletedIdleCoverage(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	calls := 0
	b.eventGCZone = func(_ byte, _ []byte, _ uint64, _ int) (int, []byte, error) {
		calls++

		return 0, nil, nil
	}

	b.runEventGC(10)
	b.runEventGC(10)

	require.Equal(t, 2, calls, "each zone must be scanned once, then remain idle")
}

func TestRunEventGC_TriggersOnWatermarkAndZoneWriteEpochEdges(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.eventGCWriteEpoch = make(map[byte]uint64)
	calls := map[byte]int{}
	b.eventGCZone = func(zone byte, _ []byte, _ uint64, _ int) (int, []byte, error) {
		calls[zone]++

		return 0, nil, nil
	}

	b.runEventGC(0)
	require.Empty(t, calls, "zero watermark cannot reclaim any valid event")

	b.runEventGC(10)
	require.Equal(t, 1, calls[readstore.PrefixMetadataIndex])
	require.Equal(t, 1, calls[readstore.PrefixEntityExists])

	b.runEventGC(10)
	require.Equal(t, 1, calls[readstore.PrefixMetadataIndex], "unchanged completed coverage stays idle")
	require.Equal(t, 1, calls[readstore.PrefixEntityExists], "unchanged completed coverage stays idle")

	b.runEventGC(20)
	require.Equal(t, 2, calls[readstore.PrefixMetadataIndex], "higher watermark restarts metadata sweep")
	require.Equal(t, 2, calls[readstore.PrefixEntityExists], "higher watermark restarts exists sweep")

	b.eventGCWriteEpoch[readstore.PrefixMetadataIndex]++
	b.runEventGC(20)
	require.Equal(t, 3, calls[readstore.PrefixMetadataIndex], "zone write restarts that zone")
	require.Equal(t, 2, calls[readstore.PrefixEntityExists], "metadata-only write does not restart exists sweep")
}

func TestRunEventGC_FreshSchedulerSweepsPositiveWatermarkAfterRestart(t *testing.T) {
	t.Parallel()

	original := newTestBuilderWithStore(t)
	original.eventGCZone = func(_ byte, _ []byte, _ uint64, _ int) (int, []byte, error) {
		return 0, nil, nil
	}
	original.runEventGC(10)

	restarted := &Builder{
		readStore: original.readStore,
		logger:    original.logger,
	}
	calls := 0
	restarted.eventGCZone = func(_ byte, _ []byte, _ uint64, _ int) (int, []byte, error) {
		calls++

		return 0, nil, nil
	}
	restarted.runEventGC(10)

	require.Equal(t, 2, calls, "fresh in-memory state conservatively sweeps both zones")
}

func TestRunEventGC_UsesStableCycleTupleAndFollowsMidCycleChanges(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.eventGCWriteEpoch = make(map[byte]uint64)

	type call struct {
		zone      byte
		resume    []byte
		watermark uint64
		epoch     uint64
	}
	var calls []call
	metadataCalls := 0
	b.eventGCZone = func(zone byte, resume []byte, watermark uint64, _ int) (int, []byte, error) {
		calls = append(calls, call{
			zone:      zone,
			resume:    append([]byte(nil), resume...),
			watermark: watermark,
			epoch:     b.eventGCCycle(zone).cycleWriteEpoch,
		})
		if zone != readstore.PrefixMetadataIndex {
			return 0, nil, nil
		}

		metadataCalls++
		if metadataCalls == 1 {
			// Model a historical event committed behind this slice's resume
			// position while the stable (watermark=10, epoch=0) cycle is active.
			b.eventGCWriteEpoch[zone] = 1

			return 0, []byte{zone, 0x42}, nil
		}

		return 0, nil, nil
	}

	b.runEventGC(10)
	b.runEventGC(20)
	b.runEventGC(20)

	var metadata []call
	for _, got := range calls {
		if got.zone == readstore.PrefixMetadataIndex {
			metadata = append(metadata, got)
		}
	}
	require.Equal(t, []call{
		{zone: readstore.PrefixMetadataIndex, watermark: 10, epoch: 0},
		{zone: readstore.PrefixMetadataIndex, resume: []byte{readstore.PrefixMetadataIndex, 0x42}, watermark: 10, epoch: 0},
		{zone: readstore.PrefixMetadataIndex, watermark: 20, epoch: 1},
	}, metadata, "mid-cycle edges schedule a later full sweep without widening the active prefix")
}

func TestRunEventGC_FollowUpCycleVisitsEventWrittenBehindResume(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.eventGCWriteEpoch = make(map[byte]uint64)
	const (
		ledger = "test"
		key    = "status"
	)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
	for _, entity := range []string{"a", "c", "d"} {
		seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, []byte(entity), 5, readstore.MetadataEventAdd)
		seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, []byte(entity), 6, readstore.MetadataEventDel)
	}

	behindAdd := cloneBytes(readstore.MetadataIndexEventKeyV(
		b.kb, ledger, readstore.NamespaceAccount, key, 1, encoded, []byte("b"), 5, readstore.MetadataEventAdd,
	))
	behindDel := cloneBytes(readstore.MetadataIndexEventKeyV(
		b.kb, ledger, readstore.NamespaceAccount, key, 1, encoded, []byte("b"), 6, readstore.MetadataEventDel,
	))

	metadataCalls := 0
	b.eventGCZone = func(zone byte, resume []byte, watermark uint64, _ int) (int, []byte, error) {
		pruned, next, err := readstore.GCEventZone(b.readStore.DB(), zone, resume, watermark, 2)
		if err != nil || zone != readstore.PrefixMetadataIndex {
			return pruned, next, err
		}

		metadataCalls++
		if metadataCalls == 1 {
			require.NotNil(t, next, "fixture must split before the late event's position")
			late := b.readStore.NewBatch()
			require.NoError(t, late.SetBytes(behindAdd, nil))
			require.NoError(t, late.SetBytes(behindDel, nil))
			require.NoError(t, late.Commit())
			b.eventGCWriteEpoch[zone]++
		}

		return pruned, next, nil
	}

	for range 20 {
		b.runEventGC(10)
		state := b.eventGCCycle(readstore.PrefixMetadataIndex)
		if state.completed && state.completedWriteEpoch == 0 && !state.active {
			break
		}
	}
	assertReadStoreValue(t, b, behindAdd, nil)
	assertReadStoreValue(t, b, behindDel, nil)

	for range 20 {
		b.runEventGC(10)
		state := b.eventGCCycle(readstore.PrefixMetadataIndex)
		if state.completed && state.completedWriteEpoch == 1 && !state.active {
			break
		}
	}
	assertReadStoreMissing(t, b, behindAdd)
	assertReadStoreMissing(t, b, behindDel)
}

func TestRunEventGC_RetriesFailedZoneWithoutBlockingOtherZone(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	calls := map[byte]int{}
	resumeKey := []byte{readstore.PrefixMetadataIndex, 0x42}
	b.eventGCZone = func(zone byte, resume []byte, watermark uint64, _ int) (int, []byte, error) {
		calls[zone]++
		if zone == readstore.PrefixMetadataIndex {
			require.Equal(t, uint64(10), watermark, "failed slice retains its captured watermark")
			switch calls[zone] {
			case 1:
				require.Nil(t, resume)

				return 0, resumeKey, nil
			case 2:
				require.Equal(t, resumeKey, resume)

				return 0, nil, errors.New("injected GC failure")
			case 3:
				require.Equal(t, resumeKey, resume, "failed slice retains its exact resume cursor")
			}
		}

		return 0, nil, nil
	}

	b.runEventGC(10)
	require.Equal(t, 1, calls[readstore.PrefixMetadataIndex])
	require.Equal(t, 1, calls[readstore.PrefixEntityExists], "other zone still completes")

	b.runEventGC(10)
	require.Equal(t, 2, calls[readstore.PrefixMetadataIndex], "failed zone retries")
	require.Equal(t, 1, calls[readstore.PrefixEntityExists], "completed zone stays idle during retry")

	b.runEventGC(10)
	require.Equal(t, 3, calls[readstore.PrefixMetadataIndex], "failed slice executes a real fail-then-success retry")

	b.runEventGC(10)
	require.Equal(t, 3, calls[readstore.PrefixMetadataIndex], "successful retry completes coverage")
}

func TestFlushWriteBatchAdvancesOnlyCommittedDirtyZoneEpochs(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const (
		ledger = "test"
		key    = "status"
		entity = "accounts:1"
	)
	reverseKey := readstore.AccountReverseMapKeyV(b.kb, ledger, entity, key, 1)
	openValue := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("open"))
	closedValue := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("closed"))

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(10)
	require.NoError(t, b.wb.ReplaceMetadataIndexV(
		b.kb, reverseKey, ledger, readstore.NamespaceAccount, key, 1,
		openValue, nil, []byte(entity),
	))
	require.NoError(t, b.flushWriteBatch())
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex])
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists])

	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(11)
	require.NoError(t, b.wb.ReplaceMetadataIndexV(
		b.kb, reverseKey, ledger, readstore.NamespaceAccount, key, 1,
		closedValue, openValue, []byte(entity),
	))
	require.NoError(t, b.flushWriteBatch())
	require.Equal(t, uint64(2), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex])
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists], "same null class emits no exists event")

	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(12)
	require.NoError(t, b.wb.ReplaceMetadataIndexV(
		b.kb, reverseKey, ledger, readstore.NamespaceAccount, key, 1,
		closedValue, closedValue, []byte(entity),
	))
	require.NoError(t, b.flushWriteBatch())
	require.Equal(t, uint64(2), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex], "no-op replacement advances no epoch")
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists])

	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(13)
	require.NoError(t, b.wb.ReplaceMetadataIndexV(
		b.kb, reverseKey, ledger, readstore.NamespaceAccount, key, 1,
		openValue, closedValue, []byte(entity),
	))
	require.NoError(t, batch.Cancel())
	b.wb.Reset()
	require.Equal(t, uint64(2), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex], "cancelled batch advances no epoch")
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists])

	batch = b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(14)
	require.NoError(t, b.wb.ReplaceMetadataIndexV(
		b.kb, reverseKey, ledger, readstore.NamespaceAccount, key, 1,
		closedValue, openValue, []byte(entity),
	))
	// Consume the session first so the helper's own Flush deterministically
	// returns an error without relying on filesystem fault injection.
	require.NoError(t, batch.Commit())
	require.Error(t, b.flushWriteBatch())
	require.Equal(t, uint64(2), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex], "failed Flush advances no epoch")
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists])
}

func TestProcessLogsSuccessfulEventCommitAdvancesDirtyZoneEpochs(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = DefaultBatchSize

	const (
		ledger = "test"
		key    = "status"
	)
	declareFieldType(t, b, ledger, key, commonpb.MetadataType_METADATA_TYPE_STRING)
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	canonical := indexes.Canonical(id)
	cfg := newLedgerIndexConfig()
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}
	b.indexConfig[ledger] = cfg
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{CurrentVersion: 1, HighWater: 1})

	writeLogToFSM(t, b, makeSavedAccountMetadataLog(1, ledger, "accounts:1", key, "open"))
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixMetadataIndex])
	require.Equal(t, uint64(1), b.eventGCWriteEpoch[readstore.PrefixEntityExists])
}

// TestGCVersionAt_PurgesForwardEidxAndRmap pins the single-version
// cleanup primitive: forward + eidx ranges go via DeleteRange (cheap
// tombstone), rmap rows that belong to the target version are
// per-key deleted. Rows at other versions and rows of other metadata
// keys are left intact.
func TestGCVersionAt_PurgesForwardEidxAndRmap(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()

	const (
		ledger  = "test"
		account = "users:1"
		ns      = readstore.NamespaceAccount
		key     = "score"
	)
	entityID := []byte(account)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(7))

	// v=1 entries (target of the GC).
	fwdV1 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd))
	eidxV1 := cloneBytes(readstore.EntityExistsEventKeyV(kb, ledger, ns, key, 1, false, entityID, 1, readstore.MetadataEventAdd))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))

	// v=2 entries (must survive).
	fwdV2 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd))
	rmapV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 2))

	// Rmap row at v=1 for a *different* metadata key — the iter
	// filter must not delete this.
	rmapV1OtherKey := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, "other", 1))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(fwdV1, nil))
	require.NoError(t, seed.SetBytes(eidxV1, nil))
	require.NoError(t, seed.SetBytes(rmapV1, encoded))
	require.NoError(t, seed.SetBytes(fwdV2, nil))
	require.NoError(t, seed.SetBytes(rmapV2, encoded))
	require.NoError(t, seed.SetBytes(rmapV1OtherKey, encoded))
	require.NoError(t, seed.Commit())

	batch := b.readStore.NewBatch()
	require.NoError(t, b.gcVersionAt(batch, kb, ledger, ns, key, 1))
	require.NoError(t, batch.Commit())

	assertReadStoreMissing(t, b, fwdV1)
	assertReadStoreMissing(t, b, eidxV1)
	assertReadStoreMissing(t, b, rmapV1)
	assertReadStoreValue(t, b, fwdV2, nil)
	assertReadStoreValue(t, b, rmapV2, encoded)
	assertReadStoreValue(t, b, rmapV1OtherKey, encoded)
}

// TestPurgeOrphanVersions_SweepsKeyspacesOutsideCurrentAndPending pins
// the boot-time recovery sweep: forward + eidx + rmap entries at
// versions != (current, pending) are purged. The cache supplies the
// version pair; the sweep enumerates 1..max(current, pending)
// skipping the live pair.
func TestPurgeOrphanVersions_SweepsKeyspacesOutsideCurrentAndPending(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()

	const (
		ledger  = "test"
		account = "users:1"
		ns      = readstore.NamespaceAccount
		key     = "score"
	)
	entityID := []byte(account)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(7))

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))
	// Cache says current=3, pending=0. Anything at v=1 or v=2 is an
	// orphan from prior switches.
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 3,
		PendingVersion: 0,
	})

	// Index must be registered in the ledger config — the sweep
	// derives (target, key) from cfg.byCanonical.
	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}
	b.indexConfig[ledger] = cfg

	// Seed orphan v=1 + v=2 entries and the live v=3 entries.
	fwdV1 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))
	fwdV2 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd))
	rmapV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 2))
	fwdV3 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 3, encoded, entityID, 1, readstore.MetadataEventAdd))
	rmapV3 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 3))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(fwdV1, nil))
	require.NoError(t, seed.SetBytes(rmapV1, encoded))
	require.NoError(t, seed.SetBytes(fwdV2, nil))
	require.NoError(t, seed.SetBytes(rmapV2, encoded))
	require.NoError(t, seed.SetBytes(fwdV3, nil))
	require.NoError(t, seed.SetBytes(rmapV3, encoded))
	require.NoError(t, seed.Commit())

	require.NoError(t, b.purgeOrphanVersions())

	// v=1 and v=2 are gone.
	assertReadStoreMissing(t, b, fwdV1)
	assertReadStoreMissing(t, b, rmapV1)
	assertReadStoreMissing(t, b, fwdV2)
	assertReadStoreMissing(t, b, rmapV2)

	// v=3 (current) survives.
	assertReadStoreValue(t, b, fwdV3, nil)
	assertReadStoreValue(t, b, rmapV3, encoded)
}

// TestPurgeOrphanVersions_PreservesPending checks that a version pair
// like (current=2, pending=3) — a rewrite in flight surviving a
// reboot — keeps BOTH keyspaces intact. The sweep must not touch
// pending_version: that's the rewrite's target and live writes
// already mirror into it.
func TestPurgeOrphanVersions_PreservesPending(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()

	const (
		ledger  = "test"
		account = "users:1"
		ns      = readstore.NamespaceAccount
		key     = "score"
	)
	entityID := []byte(account)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(7))

	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))
	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 2,
		PendingVersion: 3,
	})

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[canonical] = &commonpb.Index{Id: id}
	b.indexConfig[ledger] = cfg

	// Seed an orphan v=1 alongside the live (v=2 current, v=3 pending).
	fwdV1 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd))
	fwdV2 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd))
	fwdV3 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 3, encoded, entityID, 1, readstore.MetadataEventAdd))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(fwdV1, nil))
	require.NoError(t, seed.SetBytes(fwdV2, nil))
	require.NoError(t, seed.SetBytes(fwdV3, nil))
	require.NoError(t, seed.Commit())

	require.NoError(t, b.purgeOrphanVersions())

	assertReadStoreMissing(t, b, fwdV1)
	assertReadStoreValue(t, b, fwdV2, nil)
	assertReadStoreValue(t, b, fwdV3, nil)
}

// TestInitIndexConfig_PurgesOrphanVersionsOnBoot covers the
// crash-mid-GC scenario end-to-end: a prior process advanced
// IndexVersionState.CurrentVersion to 2 but died before its v=1 GC
// could finish. Re-running initIndexConfig must reclaim the leftover
// v=1 entries.
func TestInitIndexConfig_PurgesOrphanVersionsOnBoot(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	kb := dal.NewKeyBuilder()

	const (
		ledger  = "test"
		account = "users:1"
		ns      = readstore.NamespaceAccount
		key     = "score"
	)
	entityID := []byte(account)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(7))
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	// Persist post-switch state: current=2 (the new live keyspace),
	// pending=0 (rewrite finished cleanly).
	stateBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(stateBatch, ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 2,
		PendingVersion: 0,
	}))
	require.NoError(t, stateBatch.Commit())

	// Seed both v=1 (orphan, abandoned by the partial GC) and v=2
	// (live data).
	fwdV1 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))
	fwdV2 := cloneBytes(readstore.MetadataIndexEventKeyV(kb, ledger, ns, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(fwdV1, nil))
	require.NoError(t, seed.SetBytes(rmapV1, encoded))
	require.NoError(t, seed.SetBytes(fwdV2, nil))
	require.NoError(t, seed.Commit())

	// FSM-side LedgerInfo + bucket-scoped Index entry declare the
	// index so loadIndexRegistry registers it (the orphan sweep skips
	// unknown indexes). LedgerInfo lives under ZoneGlobal+SubGlobLedgerInfo
	// (state.SaveLedger); the Index row lives in the bucket-scoped
	// SubAttrIndex zone (registry).
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	fsmBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(fsmBatch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
	}))
	indexKey := domain.IndexKey{LedgerName: ledger, Canonical: indexes.Canonical(id)}.Bytes()
	_, err := b.attrs.Index.Set(fsmBatch, indexKey, &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		ForwardEncodingVersion: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	require.NoError(t, b.initIndexConfig(context.Background()))

	assertReadStoreMissing(t, b, fwdV1)
	assertReadStoreMissing(t, b, rmapV1)
	assertReadStoreValue(t, b, fwdV2, nil)

	// Cache reflects the persisted state.
	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(2), current)
	assert.Equal(t, uint32(0), pending)
}
