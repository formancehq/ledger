package admission

import (
	"context"
	"errors"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func businessWrite(name string) *servicepb.ApplyRequest {
	return servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: name},
		},
	})
}

func setClusterPolicyWrite(revision, limit uint64) *servicepb.ApplyRequest {
	return servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_SetClusterPolicy{
			SetClusterPolicy: &servicepb.SetClusterPolicyRequest{
				Policy: &commonpb.ClusterPolicy{Revision: revision, QueryCheckpointLimit: limit},
			},
		},
	})
}

func TestAllRequestsAreClusterPolicy(t *testing.T) {
	t.Parallel()

	policyReq := &servicepb.Request{Type: &servicepb.Request_SetClusterPolicy{
		SetClusterPolicy: &servicepb.SetClusterPolicyRequest{Policy: &commonpb.ClusterPolicy{Revision: 1}},
	}}
	businessReq := &servicepb.Request{Type: &servicepb.Request_CreateLedger{
		CreateLedger: &servicepb.CreateLedgerRequest{Name: "l"},
	}}

	require.True(t, allRequestsAreClusterPolicy([]*servicepb.Request{policyReq}))
	require.False(t, allRequestsAreClusterPolicy([]*servicepb.Request{businessReq}))
	require.False(t, allRequestsAreClusterPolicy([]*servicepb.Request{policyReq, businessReq}))
}

// TestWaitClusterPolicyReady covers the gate mechanism: it blocks while no
// policy is committed and returns as soon as one is.
func TestWaitClusterPolicyReady(t *testing.T) {
	t.Parallel()

	t.Run("blocks while no policy is committed", func(t *testing.T) {
		t.Parallel()

		store := createTestStoreWithoutPolicy(t)
		a, _ := createTestAdmission(t, store)

		ctx, cancel := context.WithTimeout(context.Background(), 100*stdtime.Millisecond)
		defer cancel()

		err := a.waitClusterPolicyReady(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cluster policy readiness")
	})

	t.Run("returns once a policy is committed", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		a, _ := createTestAdmission(t, store)

		require.NoError(t, a.waitClusterPolicyReady(context.Background()))
	})

	t.Run("unblocks when a policy is committed concurrently", func(t *testing.T) {
		t.Parallel()

		store := createTestStoreWithoutPolicy(t)
		a, _ := createTestAdmission(t, store)

		done := make(chan error, 1)
		go func() { done <- a.waitClusterPolicyReady(context.Background()) }()

		commitClusterPolicy(t, store, 1)

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-stdtime.After(5 * stdtime.Second):
			t.Fatal("waitClusterPolicyReady did not unblock after the policy was committed")
		}
	})
}

// TestAdmit_ClusterPolicyWriteReadinessGate proves the gate is wired into Admit:
// a business write is held until the policy is committed, while SetClusterPolicy
// bypasses the gate so the reconciler can establish it.
func TestAdmit_ClusterPolicyWriteReadinessGate(t *testing.T) {
	t.Parallel()

	t.Run("business write is held before the policy is committed", func(t *testing.T) {
		t.Parallel()

		store := createTestStoreWithoutPolicy(t)
		a, _ := createTestAdmissionWithReader(t, store, nil)

		ctx, cancel := context.WithTimeout(context.Background(), 100*stdtime.Millisecond)
		defer cancel()

		_, err := a.Admit(ctx, businessWrite("held-ledger"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "cluster policy readiness")
	})

	t.Run("business write proceeds once the policy is committed", func(t *testing.T) {
		t.Parallel()

		store := createTestStore(t)
		ctrl := gomock.NewController(t)
		proposer := NewMockProposer(ctrl)
		sentinel := errors.New("propose reached")
		proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil, sentinel).AnyTimes()
		a, _ := createTestAdmissionWithReader(t, store, proposer)

		ctx, cancel := context.WithTimeout(context.Background(), 3*stdtime.Second)
		defer cancel()

		_, err := a.Admit(ctx, businessWrite("new-ledger"))
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("SetClusterPolicy bypasses the gate before any policy exists", func(t *testing.T) {
		t.Parallel()

		store := createTestStoreWithoutPolicy(t)
		ctrl := gomock.NewController(t)
		proposer := NewMockProposer(ctrl)
		sentinel := errors.New("propose reached")
		proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(nil, sentinel).AnyTimes()
		a, _ := createTestAdmissionWithReader(t, store, proposer)

		ctx, cancel := context.WithTimeout(
			internalauth.WithSystemActor(context.Background(), commands.ComponentClusterPolicy),
			3*stdtime.Second,
		)
		defer cancel()

		_, err := a.Admit(ctx, setClusterPolicyWrite(1, 10))
		require.ErrorIs(t, err, sentinel)
	})
}
