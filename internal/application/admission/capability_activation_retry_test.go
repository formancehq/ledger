package admission

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestCapabilityActivationRetryRepreparesBeforeSubmission(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		store := createTestStore(t)
		proposer := NewMockProposer(gomock.NewController(t))
		attempts := 0
		proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
			attempts++
			if attempts == 1 {
				require.Equal(t, []capabilities.CapabilityID{capabilities.StrictCreateLedgerAccountTypes}, proposal.RequiredCapabilities())
				policy, err := query.ReadClusterPolicy(store)
				require.NoError(t, err)
				policy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: uint32(capabilities.StrictCreateLedgerAccountTypes), Active: true}}
				batch := store.OpenWriteSession()
				require.NoError(t, state.SaveClusterPolicy(batch, policy))
				require.NoError(t, batch.Commit())

				return nil, capabilities.ErrActivationPending
			}
			require.Equal(t, []capabilities.CapabilityID{capabilities.StrictCreateLedgerAccountTypes}, proposal.RequiredCapabilities())
			proposal.Resolve(nil, nil)
			future := futures.New[state.ApplyResult]()
			future.Resolve(state.ApplyResult{AppliedIndex: 1}, nil)

			return future, nil
		}).Times(2)
		admission, reader := createTestAdmissionWithReader(t, store, proposer)
		request := businessWrite("activation-retry")
		original := request.CloneVT()
		_, err := admission.Admit(attributedTestContext(t.Context()), request)
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
		require.True(t, original.EqualVT(request))
		require.Equal(t, int64(1), recordedActionCounts(t, reader)[mActionTotal]["create_ledger"])
	})
}

func TestCapabilityTransitionThroughProductionAdmission(t *testing.T) {
	t.Parallel()
	proposer := NewMockProposer(gomock.NewController(t))
	input := &commonpb.CapabilityActivation{Id: 7, Active: true}
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		decoded := &raftcmdpb.Proposal{}
		require.NoError(t, decoded.UnmarshalVT(proposal.Data()))
		require.True(t, input.EqualVT(decoded.GetCapabilityActivation()))
		require.Empty(t, decoded.GetOrders())
		require.False(t, proposal.HasBusinessOrders())
		require.Equal(t, []capabilities.CapabilityID{7}, proposal.ActivePolicyCapabilities())
		proposal.Resolve(nil, nil)
		future := futures.New[state.ApplyResult]()
		future.Resolve(state.ApplyResult{AppliedIndex: 1}, nil)

		return future, nil
	}).Times(1)
	admission, _ := createTestAdmissionWithReader(t, createTestStore(t), proposer)
	ctx := internalauth.WithSystemActor(t.Context(), commands.ComponentClusterPolicy)
	_, err := admission.AdmitCapabilityTransition(ctx, input)
	require.NoError(t, err)
}

func TestCapabilityTransitionRejectsDeactivationBeforeSubmission(t *testing.T) {
	t.Parallel()
	proposer := NewMockProposer(gomock.NewController(t))
	admission, _ := createTestAdmissionWithReader(t, createTestStore(t), proposer)
	ctx := internalauth.WithSystemActor(t.Context(), commands.ComponentClusterPolicy)
	_, err := admission.AdmitCapabilityTransition(ctx, &commonpb.CapabilityActivation{Id: 7})
	require.ErrorContains(t, err, "must activate")
}

func TestCapabilityActivationRetryHonorsCancellation(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		proposer := NewMockProposer(gomock.NewController(t))
		proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil, capabilities.ErrActivationPending).AnyTimes()
		admission, _ := createTestAdmissionWithReader(t, createTestStore(t), proposer)
		ctx, cancel := context.WithTimeout(attributedTestContext(t.Context()), 100*time.Millisecond)
		defer cancel()
		_, err := admission.Admit(ctx, businessWrite("activation-timeout"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestCapabilityActivationDoesNotRetryAfterSubmission(t *testing.T) {
	t.Parallel()
	proposer := NewMockProposer(gomock.NewController(t))
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		proposal.Resolve(nil, nil)
		future := futures.New[state.ApplyResult]()
		future.Resolve(state.ApplyResult{}, capabilities.ErrActivationPending)

		return future, nil
	}).Times(1)
	admission, _ := createTestAdmissionWithReader(t, createTestStore(t), proposer)
	_, err := admission.Admit(attributedTestContext(t.Context()), businessWrite("activation-submitted"))
	require.ErrorIs(t, err, capabilities.ErrActivationPending)
}
