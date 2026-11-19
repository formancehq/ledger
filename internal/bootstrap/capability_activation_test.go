package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func saveActivationTestPolicy(t *testing.T, store *dal.Store, policy *commonpb.ClusterPolicy) {
	t.Helper()
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())
}

func TestCapabilityActivationDirectCommitAndIndependentConfiguration(t *testing.T) {
	t.Parallel()
	for _, role := range []capabilities.MemberRole{capabilities.Voter, capabilities.Learner} {
		t.Run("member", func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			store, err := dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			cfg := validBaseConfig()
			ordinary := &fakeAdmission{}
			reconcileClusterPolicy(t.Context(), ordinary, store, cfg, func() bool { return true }, logging.Testing())
			saveActivationTestPolicy(t, store, ordinary.lastPolicy)
			members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: []capabilities.CapabilityID{1}}, {NodeID: 2, Role: role}}
			attempts := 0
			leader := true
			lostResponse := errors.New("lost response")
			interrupted := errors.New("leadership lost before submission")
			admit := func(_ context.Context, transition *commonpb.CapabilityActivation) (*domain.ApplyResult, error) {
				attempts++
				require.True(t, transition.GetActive())
				if attempts == 1 {
					return nil, interrupted
				}
				policy, err := query.ReadClusterPolicy(store)
				require.NoError(t, err)
				policy.CapabilityActivations = append(policy.CapabilityActivations, transition.CloneVT())
				saveActivationTestPolicy(t, store, policy)

				return nil, lostResponse
			}
			run := func() error {
				return activateReadyCapabilities(t.Context(), admit, store, func() bool { return leader }, func() []capabilities.Member { return members }, []capabilities.CapabilityID{1})
			}
			require.NoError(t, run())
			require.Zero(t, attempts, "an unsupported member must not arm activation")
			members[1].Capabilities = []capabilities.CapabilityID{1}
			require.ErrorIs(t, run(), interrupted)
			require.Equal(t, 1, attempts)
			members[1].Capabilities = nil
			require.NoError(t, run(), "an uncommitted activation is reevaluated, not resumed as durable intent")
			cfg.ClusterPolicyRevision++
			cfg.QueryCheckpointLimit++
			reconcileClusterPolicy(t.Context(), ordinary, store, cfg, func() bool { return true }, logging.Testing())
			saveActivationTestPolicy(t, store, ordinary.lastPolicy)
			require.NoError(t, store.SyncWAL())
			require.NoError(t, store.Close())
			store, err = dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
			require.NoError(t, err)
			leader = false
			require.NoError(t, run())
			require.Equal(t, 1, attempts)
			leader = true
			members[1].Capabilities = []capabilities.CapabilityID{1}
			require.ErrorIs(t, run(), lostResponse, "a new leader activates directly from fresh membership evidence")
			require.NoError(t, store.SyncWAL())
			require.NoError(t, store.Close())
			store, err = dal.NewStore(directory, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
			require.NoError(t, err)
			require.NoError(t, run())
			require.Equal(t, 2, attempts)
			applied, err := query.ReadClusterPolicy(store)
			require.NoError(t, err)
			require.Equal(t, cfg.ClusterPolicyRevision, applied.GetRevision())
			require.Equal(t, cfg.QueryCheckpointLimit, applied.GetQueryCheckpointLimit())
			require.Equal(t, []uint32{1}, capabilities.ActiveIDs(applied))
		})
	}
}

func TestCapabilityActivationRequiresMembershipEvidence(t *testing.T) {
	t.Parallel()
	store := newReconcilerTestStore(t)
	require.NoError(t, activateReadyCapabilities(t.Context(), func(context.Context, *commonpb.CapabilityActivation) (*domain.ApplyResult, error) {
		t.Fatal("empty membership must not activate capabilities")

		return nil, nil
	}, store, func() bool { return true }, func() []capabilities.Member { return nil }, []capabilities.CapabilityID{1}))
}

func TestCapabilityActivationRechecksMembershipAfterDiscovery(t *testing.T) {
	t.Parallel()
	store := newReconcilerTestStore(t)
	supported := []capabilities.CapabilityID{1}
	members := []capabilities.Member{{NodeID: 1, Role: capabilities.Voter, Capabilities: supported}}
	attempts := 0
	admit := func(_ context.Context, transition *commonpb.CapabilityActivation) (*domain.ApplyResult, error) {
		attempts++
		members = append(members, capabilities.Member{NodeID: 2, Role: capabilities.Learner})
		proposal, err := node.NewProposal(1, &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_CapabilityActivation{CapabilityActivation: transition}})
		require.NoError(t, err)
		policy, err := query.ReadClusterPolicy(store)
		require.NoError(t, err)

		return nil, validateProposalCapabilityAdmissionWithSupported(policy, proposal, members, supported)
	}
	err := activateReadyCapabilities(t.Context(), admit, store, func() bool { return true }, func() []capabilities.Member { return members }, supported)
	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
	require.Equal(t, 1, attempts)
	policy, err := query.ReadClusterPolicy(store)
	require.NoError(t, err)
	require.Empty(t, policy.GetCapabilityActivations())
	require.NoError(t, activateReadyCapabilities(t.Context(), admit, store, func() bool { return true }, func() []capabilities.Member { return members }, supported))
	require.Equal(t, 1, attempts)
}
