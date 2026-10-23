package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestReadClusterPolicyControlStateIntegrity(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name         string
		schema       uint32
		policy       *commonpb.ClusterPolicy
		capabilities *commonpb.CapabilityState
		wantError    string
	}{
		{name: "legacy", schema: 2, policy: &commonpb.ClusterPolicy{Revision: 7, CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}}}},
		{name: "extracted", schema: 3, policy: &commonpb.ClusterPolicy{Revision: 7}, capabilities: &commonpb.CapabilityState{Activations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}}}},
		{name: "missing capabilities", schema: 3, policy: &commonpb.ClusterPolicy{Revision: 7}, wantError: "missing capability state"},
		{name: "missing policy", schema: 3, capabilities: &commonpb.CapabilityState{}, wantError: "invalid split control state"},
		{name: "duplicated ownership", schema: 3, policy: &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 1, Active: true}}}, capabilities: &commonpb.CapabilityState{}, wantError: "invalid split control state"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t)
			batch := store.OpenWriteSession()
			require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterPersistent, dal.SubGlobPersistedConfig}, &commonpb.PersistedConfig{StorageSchemaVersion: testCase.schema}))
			if testCase.policy != nil {
				require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterPolicy}, testCase.policy))
			}
			if testCase.capabilities != nil {
				require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobCapabilityState}, testCase.capabilities))
			}
			require.NoError(t, batch.Commit())
			policy, err := query.ReadClusterPolicy(store)
			if testCase.wantError != "" {
				require.ErrorContains(t, err, testCase.wantError)

				return
			}
			require.NoError(t, err)
			expected := testCase.policy.CloneVT()
			if testCase.capabilities != nil {
				expected.CapabilityActivations = testCase.capabilities.GetActivations()
			}
			require.True(t, expected.EqualVT(policy))
		})
	}
}

func TestReadLastAppliedIndex(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Initial value should be 0
	lastIndex, err := query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIndex)

	// Create batch with index 5
	batch := s.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "test", &ledgerpb.LedgerInfo{
		Name: "test",
	}))
	require.NoError(t, state.SetAppliedIndex(batch, 5))
	require.NoError(t, batch.Commit())

	// Verify last applied index updated
	lastIndex, err = query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastIndex)

	// Create another batch with index 10
	batch = s.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "test2", &ledgerpb.LedgerInfo{
		Name: "test2",
	}))
	require.NoError(t, state.SetAppliedIndex(batch, 10))
	require.NoError(t, batch.Commit())

	lastIndex, err = query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(10), lastIndex)
}
