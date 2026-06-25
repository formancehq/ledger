package indexbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

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
	fwdV1 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 1, encoded, entityID))
	eidxV1 := cloneBytes(readstore.EntityExistsKeyV(kb, ledger, ns, key, 1, false, entityID))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))

	// v=2 entries (must survive).
	fwdV2 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 2, encoded, entityID))
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
	fwdV1 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 1, encoded, entityID))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))
	fwdV2 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 2, encoded, entityID))
	rmapV2 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 2))
	fwdV3 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 3, encoded, entityID))
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
	fwdV1 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 1, encoded, entityID))
	fwdV2 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 2, encoded, entityID))
	fwdV3 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 3, encoded, entityID))

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
	fwdV1 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 1, encoded, entityID))
	rmapV1 := cloneBytes(readstore.AccountReverseMapKeyV(kb, ledger, account, key, 1))
	fwdV2 := cloneBytes(readstore.MetadataIndexKeyV(kb, ledger, ns, key, 2, encoded, entityID))

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
	require.NoError(t, state.SaveLedger(fsmBatch, &commonpb.LedgerInfo{
		Name: ledger,
	}))
	indexKey := domain.IndexKey{LedgerName: ledger, Canonical: indexes.Canonical(id)}.Bytes()
	_, err := b.attrs.Index.Set(fsmBatch, indexKey, &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY,
		ForwardEncodingVersion: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	b.initIndexConfig(context.Background())

	assertReadStoreMissing(t, b, fwdV1)
	assertReadStoreMissing(t, b, rmapV1)
	assertReadStoreValue(t, b, fwdV2, nil)

	// Cache reflects the persisted state.
	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(2), current)
	assert.Equal(t, uint32(0), pending)
}
