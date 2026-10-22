package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

type capabilityPolicyTestProposer struct{ called bool }

func (p *capabilityPolicyTestProposer) Propose(context.Context, *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	p.called = true

	return futures.New[state.ApplyResult](), nil
}

func capabilityPolicyProposal(t *testing.T) *node.Proposal {
	t.Helper()

	data, err := (&raftcmdpb.Proposal{
		ExecutionPlan:        &raftcmdpb.ExecutionPlan{},
		RequiredCapabilities: []uint32{99},
	}).MarshalVT()
	require.NoError(t, err)

	return node.NewProposal(1, data)
}

func TestCapabilityPolicyProposerBlocksInactiveCapability(t *testing.T) {
	delegate := new(capabilityPolicyTestProposer)
	proposer := NewCapabilityPolicyProposer(delegate, func() (*commonpb.ClusterPolicy, error) {
		return &commonpb.ClusterPolicy{
			CapabilityActivationArmed: true,
		}, nil
	}, nil, false)

	_, err := proposer.Propose(context.Background(), capabilityPolicyProposal(t))
	require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
	require.False(t, delegate.called)
}

func TestCapabilityPolicyProposerAllowsActiveTechnicalCapability(t *testing.T) {
	delegate := new(capabilityPolicyTestProposer)
	proposer := NewCapabilityPolicyProposer(delegate, func() (*commonpb.ClusterPolicy, error) {
		return &commonpb.ClusterPolicy{
			CapabilityActivations: []*commonpb.CapabilityActivation{{
				Id:     99,
				Active: true,
			}},
		}, nil
	}, nil, false)

	_, err := proposer.Propose(context.Background(), capabilityPolicyProposal(t))
	require.NoError(t, err)
	require.True(t, delegate.called)
}
