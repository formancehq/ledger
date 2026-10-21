package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

type capabilityTestProposer struct{ called bool }

func (p *capabilityTestProposer) Propose(context.Context, *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.called = true
	return futures.New[state.ApplyResult](), nil
}

var _ plan.Proposer = (*capabilityTestProposer)(nil)

type barrierTestProposer struct{}

func (barrierTestProposer) InitialIndex() uint64 { return 1 }

func (barrierTestProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	if err := capabilities.ValidateFinalProposal(proposal.Data(), proposal.DeclaredCapabilities()); err != nil {
		return nil, err
	}
	proposal.Resolve(nil, nil)
	future := futures.New[state.ApplyResult]()
	future.Resolve(state.ApplyResult{AppliedIndex: 1}, nil)
	return future, nil
}

type allowWrites struct{}

func (allowWrites) CheckWritesAllowed() error { return nil }

var _ health.WriteGate = allowWrites{}

func TestBarrierStampsSemanticCapabilities(t *testing.T) {
	a, _ := createTestAdmission(t, createTestStore(t))
	a.writeGate = allowWrites{}
	a.proposer = barrierTestProposer{}

	_, err := a.Barrier(context.Background())
	require.NoError(t, err)
}

func TestCapabilityPolicyProposerBlocksAllWritesAtFinalBoundary(t *testing.T) {
	delegate := new(capabilityTestProposer)
	proposer := capabilityPolicyProposer{
		delegate: delegate,
		readPolicy: func() (*commonpb.ClusterPolicy, error) {
			return &commonpb.ClusterPolicy{
				CapabilityActivationArmed:    true,
				CapabilityAdmissionPolicy:    commonpb.CapabilityAdmissionPolicy_CAPABILITY_ADMISSION_POLICY_ALL_WRITES,
				CapabilityMembershipRevision: 1,
			}, nil
		},
		businessWrite: true,
	}
	data, err := (&raftcmdpb.Proposal{}).MarshalVT()
	require.NoError(t, err)
	_, err = proposer.Propose(context.Background(), node.NewProposal(1, data))
	require.Error(t, err)
	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
	require.False(t, delegate.called)
}

func TestCapabilityPolicyProposerAllowsFeatureOnlyUnrelatedWrite(t *testing.T) {
	delegate := new(capabilityTestProposer)
	proposer := capabilityPolicyProposer{
		delegate: delegate,
		readPolicy: func() (*commonpb.ClusterPolicy, error) {
			return &commonpb.ClusterPolicy{
				CapabilityActivationArmed:    true,
				CapabilityAdmissionPolicy:    commonpb.CapabilityAdmissionPolicy_CAPABILITY_ADMISSION_POLICY_FEATURE_ONLY,
				CapabilityMembershipRevision: 1,
			}, nil
		},
		businessWrite: true,
	}
	data, err := (&raftcmdpb.Proposal{}).MarshalVT()
	require.NoError(t, err)
	_, err = proposer.Propose(context.Background(), node.NewProposal(1, data))
	require.NoError(t, err)
	require.True(t, delegate.called)
}
