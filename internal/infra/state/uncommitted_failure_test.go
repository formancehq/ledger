package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func clusterPolicyOrder(policy *commonpb.ClusterPolicy) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
		SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy},
		}},
	}}
}

func TestApplyProposal_InvalidClusterPolicyIsUncommittedAndRetryable(t *testing.T) {
	t.Parallel()
	fsm, store, _ := newTestMachine(t)
	before := fsm.State.NextAuditSequenceID

	proposal := makeProposal(1, clusterPolicyOrder(nil))
	proposal.Idempotency = &commonpb.Idempotency{Key: "policy-retry"}
	result, err := fsm.ApplyEntries(context.Background(), store, makeEntry(t, 1, proposal))
	require.NoError(t, err)
	var invalid *domain.ErrClusterPolicyInvalid
	require.ErrorAs(t, result.Results[0].Error, &invalid)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, before, fsm.State.NextAuditSequenceID)
	_, frozen := fsm.Registry.Idempotency.Get("policy-retry")
	require.False(t, frozen)

	proposal = makeProposal(2, clusterPolicyOrder(idempotencyTestPolicy(2, 1000)))
	proposal.Idempotency = &commonpb.Idempotency{Key: "policy-retry"}
	result, err = fsm.ApplyEntries(context.Background(), store, makeEntry(t, 2, proposal))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	require.True(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, before+1, fsm.State.NextAuditSequenceID)
}

func TestApplyProposal_ClusterPolicyRevisionConflictIsUncommitted(t *testing.T) {
	t.Parallel()
	fsm, store, _ := newTestMachine(t)
	ctx := context.Background()
	policy := idempotencyTestPolicy(2, 1000)
	result, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, clusterPolicyOrder(policy))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	before := fsm.State.NextAuditSequenceID

	conflicting := idempotencyTestPolicy(2, 2000)
	proposal := makeProposal(2, clusterPolicyOrder(conflicting))
	proposal.Idempotency = &commonpb.Idempotency{Key: "policy-conflict"}
	result, err = fsm.ApplyEntries(ctx, store, makeEntry(t, 2, proposal))
	require.NoError(t, err)
	var conflict *domain.ErrClusterPolicyRevisionConflict
	require.ErrorAs(t, result.Results[0].Error, &conflict)
	require.False(t, result.Results[0].AuditEntryWritten)
	require.Equal(t, before, fsm.State.NextAuditSequenceID)
	_, frozen := fsm.Registry.Idempotency.Get("policy-conflict")
	require.False(t, frozen)
}
