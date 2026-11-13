package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// A fresh store has no committed policy, so recovery leaves the revision-0
// default; a persisted policy is recovered verbatim into FSMState.
func TestRecoverState_ClusterPolicyRoundtrip(t *testing.T) {
	t.Parallel()

	// Deliberately the un-seeded harness: this test is about what recovery
	// itself produces, so the machine must be genuinely fresh.
	machine, store, _ := newTestMachineWithThreshold(t, 1000)

	require.NotNil(t, machine.State.ClusterPolicy)
	require.Equal(t, uint64(0), machine.State.ClusterPolicy.GetRevision(),
		"a fresh store recovers the revision-0 default (no policy committed)")

	policy := &commonpb.ClusterPolicy{Revision: 4, IdempotencyTtlMicros: 5000, QueryCheckpointLimit: 12}
	batch := store.OpenWriteSession()
	require.NoError(t, SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())

	require.NoError(t, NewRecovery(machine, store).RecoverState())

	require.True(t, policy.EqualVT(machine.State.ClusterPolicy),
		"the persisted policy must be recovered verbatim into FSMState")
	require.True(t, policy.EqualVT(machine.CommittedClusterPolicy()),
		"recovery must publish the committed policy for admission")
}

func TestCommittedClusterPolicyPublishedAfterBatchCommit(t *testing.T) {
	t.Parallel()
	machine, store, _ := newTestMachineWithThreshold(t, 1000)
	require.NoError(t, NewRecovery(machine, store).RecoverState())
	before := machine.CommittedClusterPolicy()
	require.Equal(t, uint64(0), before.GetRevision())

	policy := idempotencyTestPolicy(1, 1000)
	prepared, err := machine.PrepareEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, clusterPolicyOrder(policy))))
	require.NoError(t, err)
	defer prepared.Close()
	require.Equal(t, uint64(1), machine.State.ClusterPolicy.GetRevision())
	require.Equal(t, uint64(0), machine.CommittedClusterPolicy().GetRevision(),
		"admission must not see a policy staged by PrepareEntries")

	require.NoError(t, machine.CommitPreparedBatch(context.Background(), prepared))
	require.True(t, policy.EqualVT(machine.CommittedClusterPolicy()))
}

func TestRecoverStatePublishesRecoveredState(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachineWithThreshold(t, 1000)
	policy := &commonpb.ClusterPolicy{
		Revision:              4,
		CapabilityActivations: []*commonpb.CapabilityActivation{{Id: 17, Active: true}},
	}
	batch := store.OpenWriteSession()
	require.NoError(t, SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())

	var recovered *FSMState
	recovery := NewRecovery(machine, store, WithStateRecovered(func(state *FSMState) error {
		recovered = state

		return nil
	}))
	require.NoError(t, recovery.RecoverState())
	require.NotNil(t, recovered)
	require.True(t, policy.EqualVT(recovered.ClusterPolicy))
}

// NewFSMState seeds a non-nil revision-0 default so the apply path and the
// readiness gate can read the policy before any is committed.
func TestNewFSMState_ClusterPolicyDefault(t *testing.T) {
	t.Parallel()

	s := NewFSMState("test-cluster")
	require.NotNil(t, s.ClusterPolicy)
	require.Equal(t, uint64(0), s.ClusterPolicy.GetRevision())
}
