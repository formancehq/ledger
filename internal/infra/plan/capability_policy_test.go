package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

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

	proposal := &raftcmdpb.Proposal{
		ExecutionPlan: &raftcmdpb.ExecutionPlan{},
	}
	result, err := node.NewProposal(1, proposal)
	require.NoError(t, err)

	return result
}

func TestCapabilityPolicyProposerAllowsUnrelatedProposal(t *testing.T) {
	delegate := new(capabilityPolicyTestProposer)
	proposer := NewCapabilityPolicyProposer(delegate, func() (*commonpb.ClusterPolicy, error) {
		return &commonpb.ClusterPolicy{}, nil
	}, nil, false)

	_, err := proposer.Propose(context.Background(), capabilityPolicyProposal(t))
	require.NoError(t, err)
	require.True(t, delegate.called)
}
