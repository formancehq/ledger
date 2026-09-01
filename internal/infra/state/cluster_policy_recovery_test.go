package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// A fresh store has no committed policy, so recovery leaves the revision-0
// default; a persisted policy is recovered verbatim into FSMState.
func TestRecoverState_ClusterPolicyRoundtrip(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)

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
}

// NewFSMState seeds a non-nil revision-0 default so the apply path and the
// readiness gate can read the policy before any is committed.
func TestNewFSMState_ClusterPolicyDefault(t *testing.T) {
	t.Parallel()

	s := NewFSMState("test-cluster")
	require.NotNil(t, s.ClusterPolicy)
	require.Equal(t, uint64(0), s.ClusterPolicy.GetRevision())
}
