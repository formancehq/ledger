package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestWriteSetMergeRejectsUnbalancedVolumeUpdate protects the primary
// double-entry guard over all volume updates. The production order processors
// can only build balanced posting pairs; constructing the broken intermediate
// state directly is intentional fault injection at the guard's boundary.
func TestWriteSetMergeRejectsUnbalancedVolumeUpdate(t *testing.T) {
	t.Parallel()

	buf, _, dataStore := newTestBuffer(t)
	key := domain.NewVolumeKey("test", "destination", "USD", "")
	buf.Volumes().Put(key, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(0),
	})
	buf.gatedLedgerTypes = map[string]gatedLedgerType{
		"test": {found: true},
	}

	batch := dataStore.OpenWriteSession()
	err := buf.Merge(batch, nil)
	require.NoError(t, batch.Cancel())

	var violation *ErrDoubleEntryInvariantViolated
	require.ErrorAs(t, err, &violation)
	require.Equal(t, "100", violation.InputSum)
	require.Equal(t, "0", violation.OutputSum)

	// DerivedKeyStore.Merge updates the cache before this invariant runs. A
	// Merge error is terminal to the production applier, so cache rollback is
	// not part of the recoverable contract and is deliberately not asserted.
	// The rejected update must never cross the Pebble commit boundary.
	persisted, err := buf.attrs.Volume.Get(dataStore, key.Bytes())
	require.NoError(t, err)
	require.Nil(t, persisted)
}

// The all-update check passes here; the inconsistent transient partition must
// be rejected specifically by the persisted-update check.
func TestWriteSetMergeRejectsUnbalancedPersistedVolumeUpdate(t *testing.T) {
	t.Parallel()
	buf, machine, dataStore := newTestBuffer(t)
	machine.sentinelMode = true
	machine.sentinelTracer = NewSentinelTracer(machine.logger)
	buf.gatedLedgerTypes = gatedTypesFor(&commonpb.LedgerInfo{Name: "test", AccountTypes: map[string]*commonpb.AccountType{"transient": {Name: "transient", Pattern: "transient:{id}", Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT}}})
	updates := []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{sentinelVolume("transient:source", "USD", "", 0, 0, 0, 10), sentinelVolume("destination", "USD", "", 0, 0, 10, 0)}
	require.NoError(t, checkDoubleEntryInvariant(updates))
	for _, update := range updates {
		buf.Volumes().Put(update.Key, update.New)
	}
	batch := dataStore.OpenWriteSession()
	err := buf.Merge(batch, nil)
	require.NoError(t, batch.Cancel())
	require.ErrorContains(t, err, "persisted double-entry invariant violated (transient excluded)")
	var violation *ErrDoubleEntryInvariantViolated
	require.ErrorAs(t, err, &violation)
	require.Equal(t, "10", violation.InputSum)
	require.Equal(t, "0", violation.OutputSum)
}
