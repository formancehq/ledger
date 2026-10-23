package plan

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"unsafe"

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

	proposal := &raftcmdpb.Proposal{
		ExecutionPlan: &raftcmdpb.ExecutionPlan{},
	}
	result, err := node.NewProposal(1, proposal)
	require.NoError(t, err)

	return result
}

// capabilityPolicyProposalWithRequired is a package-local seam for exercising
// the policy wrapper until the capability registry contains a real feature.
// Production proposals can only get this metadata through node.NewProposal.
func capabilityPolicyProposalWithRequired(t *testing.T, required ...capabilities.CapabilityID) *node.Proposal {
	t.Helper()

	proposal := capabilityPolicyProposal(t)
	field := reflect.ValueOf(proposal).Elem().FieldByName("requiredCapabilities")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(required))

	return proposal
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

func TestCapabilityPolicyProposerRejectsPolicyReadFailureForBothProposalModes(t *testing.T) {
	t.Parallel()

	for _, businessWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical", true: "business"}[businessWrite], func(t *testing.T) {
			delegate := new(capabilityPolicyTestProposer)
			proposer := NewCapabilityPolicyProposer(delegate, func() (*commonpb.ClusterPolicy, error) {
				return nil, errors.New("policy unavailable")
			}, nil, businessWrite)

			_, err := proposer.Propose(context.Background(), capabilityPolicyProposal(t))
			require.ErrorContains(t, err, "policy unavailable")
			require.False(t, delegate.called)
		})
	}
}

func TestCapabilityPolicyProposerRejectsInactiveCapabilityForBothProposalModes(t *testing.T) {
	t.Parallel()

	for _, businessWrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "technical", true: "business"}[businessWrite], func(t *testing.T) {
			delegate := new(capabilityPolicyTestProposer)
			proposer := NewCapabilityPolicyProposer(delegate, func() (*commonpb.ClusterPolicy, error) {
				return &commonpb.ClusterPolicy{}, nil
			}, nil, businessWrite)

			_, err := proposer.Propose(context.Background(), capabilityPolicyProposalWithRequired(t, 99))
			require.ErrorIs(t, err, capabilities.ErrCapabilityUnsupported)
			require.False(t, delegate.called)
		})
	}
}
