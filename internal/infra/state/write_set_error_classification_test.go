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

// --- C1: partitionVolumes ---------------------------------------------------

// TestPartitionVolumes_MissingLedgerIsSoftKept pins the documented soft
// outcome: a volume for a ledger with no account-type info (absent ledger)
// defaults to "kept", no error.
func TestPartitionVolumes_MissingLedgerIsSoftKept(t *testing.T) {
	t.Parallel()

	buf, _, _ := newTestBuffer(t)

	key := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "ghost", Account: "acc"}, Asset: "USD"}
	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          key,
			CanonicalKey: key.Bytes(),
			New:          &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)},
		},
	}

	result, err := buf.partitionVolumes(updates)
	require.NoError(t, err)
	require.Len(t, result.kept, 1)
	require.Empty(t, result.purged)
	require.Empty(t, result.transient)
}

// TestPartitionVolumes_StorageFaultPropagates pins that a non-ErrNotFound
// fault on the ledger read surfaces loudly instead of silently keeping the
// volume.
func TestPartitionVolumes_StorageFaultPropagates(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	const ledgerName = "faulty"
	ledgerKey := domain.LedgerKey{Name: ledgerName}
	injectTagCollision(t, machine.Registry.Ledgers.KeyStore(), ledgerKey.Bytes(), &commonpb.LedgerInfo{Name: ledgerName})

	key := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: "acc"}, Asset: "USD"}
	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{
		{
			Key:          key,
			CanonicalKey: key.Bytes(),
			New:          &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(0), Output: commonpb.NewUint256FromUint64(0)},
		},
	}

	_, err := buf.partitionVolumes(updates)
	require.Error(t, err)

	var collision *attributes.ErrCollisionDetected
	require.ErrorAs(t, err, &collision, "a non-ErrNotFound ledger read fault must propagate out of partitionVolumes")
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
