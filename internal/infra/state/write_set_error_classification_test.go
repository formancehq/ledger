package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// EN-1522 gap C — classify expected absence vs real error. Each site keeps
// domain.ErrNotFound as a documented soft outcome and propagates every other
// error loudly (invariant #7). The fault is injected as a genuine
// storage/cache fault (tag collision) via injectTagCollision.

// injectTagCollision forces the next KeyStore.Get for canonical to return a
// non-ErrNotFound *ErrCollisionDetected by storing an entry under the key's
// U128 id but with a deliberately mismatched tag. This is the state-package
// fault-injection seam for the invariant-#7 error-classification tests below:
// a genuine storage/cache fault, distinct from an absence.
func injectTagCollision[K attributes.Key, V any](t *testing.T, ks *attributes.KeyStore[K, V], canonical []byte, data V) {
	t.Helper()

	id, tag := attributes.NewKeyHasher().MakeKey(canonical)
	ks.M.Put(id, attributes.Entry[V]{Tag: tag ^ 0xBEEF, Data: data})
}

// --- C1: partitionVolumes ---------------------------------------------------

// volumeUpdate builds a single zero-balance volume update for ledger/account.
func volumeUpdate(ledgerName, account string) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	key := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account}, Asset: "USD"}

	return []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          key,
			CanonicalKey: key.Bytes(),
			New:          &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)},
		},
	}
}

// TestPartitionVolumes_MissingLedgerIsSoftKept pins the documented soft
// outcome: when the gated pass resolved the ledger as genuinely absent
// (found:false), the volume defaults to "kept", no error.
func TestPartitionVolumes_MissingLedgerIsSoftKept(t *testing.T) {
	t.Parallel()

	buf, _, _ := newTestBuffer(t)

	buf.gatedLedgerTypes = map[string]gatedLedgerType{"ghost": {}}

	result, err := buf.partitionVolumes(volumeUpdate("ghost", "acc"))
	require.NoError(t, err)
	require.Len(t, result.kept, 1)
	require.Empty(t, result.purged)
	require.Empty(t, result.transient)
}

// TestPartitionVolumes_UngatedLedgerFailsLoudly is the invariant-#9 regression:
// partitionVolumes must never classify a volume off a ledger row it can reach
// in the raw parent KeyStore. The ledger is seeded directly into the parent
// cache carrying an EPHEMERAL type, but no gated pass ran — so gatedLedgerTypes
// is empty. Pre-fix, partitionVolumes read the parent through getLedgerData,
// matched EPHEMERAL and purged the zero-balance volume; this test therefore
// cannot pass on a raw parent-cache hit.
func TestPartitionVolumes_UngatedLedgerFailsLoudly(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const ledgerName = "ungated"
	ledger := &commonpb.LedgerInfo{
		Name: ledgerName,
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"cache": {
				Name:        "cache",
				Pattern:     "acc",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		},
	}
	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
		(&domain.LedgerKey{Name: ledgerName}).Bytes(),
		ledger,
	)
	require.NoError(t, err)

	// No gated pass ran: the map is empty even though the parent cache would
	// happily serve this ledger.
	require.Empty(t, buf.gatedLedgerTypes)

	result, err := buf.partitionVolumes(volumeUpdate(ledgerName, "acc"))
	require.Error(t, err, "an ungated ledger must not be classified off the parent cache")
	require.ErrorContains(t, err, "invariant:")
	require.ErrorContains(t, err, ledgerName)
	require.Empty(t, result.purged, "the volume must not be purged off an ungated EPHEMERAL match")
}

// TestValidateTransientVolumes_LedgerStorageFaultPropagates pins that a
// non-ErrNotFound fault on the ledger read surfaces loudly instead of being
// treated as an absence. The read moved here from partitionVolumes, which now
// consumes this pass's gated resolution.
func TestValidateTransientVolumes_LedgerStorageFaultPropagates(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const ledgerName = "faulty"
	ledgerKey := domain.LedgerKey{Name: ledgerName}
	injectTagCollision(t, machine.Registry.Ledgers.KeyStore(), ledgerKey.Bytes(), &commonpb.LedgerInfo{Name: ledgerName})

	volKey := domain.NewVolumeKey(ledgerName, "acc", "USD", "")
	buf.Derived.Volumes.Put(volKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	})

	lid, _ := attributes.MakeKey(ledgerKey.Bytes())
	vid, _ := attributes.MakeKey(volKey.Bytes())
	scope, err := NewScopeFactory(
		buf,
		&raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(lid, dal.SubAttrLedger),
			declareTestPlan(vid, dal.SubAttrVolume),
		}},
		machine.logger,
		machine.preloadMissCounter,
		1,
	).NewProposalScope()
	require.NoError(t, err)

	describ := buf.ValidateTransientVolumes(scope)
	require.NotNil(t, describ, "a non-ErrNotFound ledger read fault must surface")

	_, ok := describ.(*domain.ErrStorageOperation)
	require.True(t, ok, "expected a storage fault, got %T", describ)
}

// TestValidateTransientVolumes_PublishesGatedTypes pins the carry-forward
// contract partitionVolumes depends on: every dirty volume's ledger gets an
// entry, in both the resolved and the absent case.
func TestValidateTransientVolumes_PublishesGatedTypes(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const presentLedger = "present"
	ledger := &commonpb.LedgerInfo{
		Name: presentLedger,
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"cache": {
				Name:        "cache",
				Pattern:     "acc",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
			},
		},
	}
	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
		(&domain.LedgerKey{Name: presentLedger}).Bytes(),
		ledger,
	)
	require.NoError(t, err)
	buf.Derived.Ledgers.Put(domain.LedgerKey{Name: presentLedger}, ledger)

	presentVol := domain.NewVolumeKey(presentLedger, "acc", "USD", "")
	absentVol := domain.NewVolumeKey("absent", "acc", "USD", "")
	zero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	buf.Derived.Volumes.Put(presentVol, zero)
	buf.Derived.Volumes.Put(absentVol, zero)

	lid, _ := attributes.MakeKey((&domain.LedgerKey{Name: presentLedger}).Bytes())
	absentLid, _ := attributes.MakeKey((&domain.LedgerKey{Name: "absent"}).Bytes())
	presentVid, _ := attributes.MakeKey(presentVol.Bytes())
	absentVid, _ := attributes.MakeKey(absentVol.Bytes())
	scope, err := NewScopeFactory(
		buf,
		&raftcmdpb.ExecutionPlan{Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(lid, dal.SubAttrLedger),
			declareTestPlan(absentLid, dal.SubAttrLedger),
			declareTestPlan(presentVid, dal.SubAttrVolume),
			declareTestPlan(absentVid, dal.SubAttrVolume),
		}},
		machine.logger,
		machine.preloadMissCounter,
		1,
	).NewProposalScope()
	require.NoError(t, err)

	require.Nil(t, buf.ValidateTransientVolumes(scope))

	present, ok := buf.gatedLedgerTypes[presentLedger]
	require.True(t, ok, "a resolved ledger must be published")
	require.True(t, present.found)
	require.NotEmpty(t, present.compiled)

	absent, ok := buf.gatedLedgerTypes["absent"]
	require.True(t, ok, "a gate-resolved absence must be published, not skipped")
	require.False(t, absent.found)
}

// --- C3: GetSinkConfig ------------------------------------------------------

// TestGetSinkConfig_AbsentIsSoftNil pins the documented soft outcome: an
// absent sink config returns (nil, nil).
func TestGetSinkConfig_AbsentIsSoftNil(t *testing.T) {
	t.Parallel()

	buf, _, _ := newTestBuffer(t)

	cfg, err := buf.GetSinkConfig("nonexistent")
	require.NoError(t, err)
	require.Nil(t, cfg)
}

// TestGetSinkConfig_StorageFaultPropagates pins that a non-ErrNotFound fault
// surfaces instead of being reported as "no sink config".
func TestGetSinkConfig_StorageFaultPropagates(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const name = "faulty-sink"
	injectTagCollision(t, machine.Registry.SinkConfigs.KeyStore(), domain.SinkConfigKey{Name: name}.Bytes(), &commonpb.SinkConfig{Name: name})

	cfg, err := buf.GetSinkConfig(name)
	require.Error(t, err)
	require.Nil(t, cfg)

	var collision *attributes.ErrCollisionDetected
	require.ErrorAs(t, err, &collision)
}

// --- C4: NumscriptVersionExists ---------------------------------------------

// TestNumscriptVersionExists_AbsentIsSoftFalse pins the documented soft
// outcome: an absent version returns (false, nil).
func TestNumscriptVersionExists_AbsentIsSoftFalse(t *testing.T) {
	t.Parallel()

	buf, _, _ := newTestBuffer(t)

	exists, err := buf.NumscriptVersionExists("ledger", "script", "1.0.0")
	require.NoError(t, err)
	require.False(t, exists)
}

// TestNumscriptVersionExists_StorageFaultPropagates pins that a non-ErrNotFound
// fault surfaces instead of being reported as "version does not exist" (which
// would let the caller write a duplicate).
func TestNumscriptVersionExists_StorageFaultPropagates(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const (
		ledgerName = "ledger"
		scriptName = "script"
		version    = "1.0.0"
	)
	entryKey := domain.NumscriptEntryKey{LedgerName: ledgerName, Name: scriptName, Version: version}
	injectTagCollision(t, machine.Registry.NumscriptContents.KeyStore(), entryKey.Bytes(), &commonpb.NumscriptInfo{Name: scriptName, Version: version})

	exists, err := buf.NumscriptVersionExists(ledgerName, scriptName, version)
	require.Error(t, err)
	require.False(t, exists)

	var collision *attributes.ErrCollisionDetected
	require.ErrorAs(t, err, &collision)
}

// --- C2: ValidateTransientVolumes base read ---------------------------------

// TestValidateTransientVolumes_BaseReadFaultSurfaces pins that a
// non-ErrNotFound fault on the transient base-volume read is reported as a
// storageFault (wrapped ErrStorageOperation) rather than letting the
// zero-balance assertion run on an unread base.
func TestValidateTransientVolumes_BaseReadFaultSurfaces(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const ledgerName = "transient-fault"

	// Seed a ledger whose account type marks "staging:{id}" TRANSIENT.
	ledgerInfo := &commonpb.LedgerInfo{
		Id:   1,
		Name: ledgerName,
		AccountTypes: map[string]*commonpb.AccountType{
			"staging": {
				Name:        "staging",
				Pattern:     "staging:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
			},
		},
	}
	ledgerKey := domain.LedgerKey{Name: ledgerName}
	_, _, err := machine.Registry.Ledgers.KeyStore().Put(ledgerKey.Bytes(), ledgerInfo)
	require.NoError(t, err)

	// A dirty transient volume in the overlay.
	volKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "staging:tx1"}, Asset: "USD"}
	buf.Derived.Volumes.Put(volKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(50),
		Output: commonpb.NewUint256FromUint64(50),
	})

	// Inject a fault on the BASE (parent KeyStore) read for that key.
	injectTagCollision(t, machine.Registry.Volumes.KeyStore(), volKey.Bytes(), &raftcmdpb.VolumePair{})

	// Build a proposal-wide gated scope that admits the ledger and the volume key.
	ledgerID, _ := attributes.MakeKey(ledgerKey.Bytes())
	volID, _ := attributes.MakeKey(volKey.Bytes())
	plan := &raftcmdpb.ExecutionPlan{
		Attributes: []*raftcmdpb.AttributeCoverage{
			declareTestPlan(ledgerID, dal.SubAttrLedger),
			declareTestPlan(volID, dal.SubAttrVolume),
		},
	}
	scope, err := NewScopeFactory(buf, plan, machine.logger, machine.preloadMissCounter, 1).NewProposalScope()
	require.NoError(t, err)

	derr := buf.ValidateTransientVolumes(scope)
	require.NotNil(t, derr, "a base-read fault must surface as a storage fault, not a silent skip")

	var storageErr *domain.ErrStorageOperation
	require.ErrorAs(t, derr, &storageErr)
	require.Equal(t, "reading transient base volume", storageErr.Operation)
}
