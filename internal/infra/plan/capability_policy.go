package plan

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// CapabilityPolicyProposer rechecks the committed capability policy against
// the capabilities derived by the producer from the final proposal immediately
// before handing it to the Raft proposer.
type CapabilityPolicyProposer struct {
	delegate      Proposer
	readPolicy    func() (*commonpb.ClusterPolicy, error)
	readMembers   func() []capabilities.Member
	businessWrite bool
}

// NewCapabilityPolicyProposer wraps a proposer with the replicated policy
// admission check. The callback must read the committed policy projection, not
// process-local configuration.
func NewCapabilityPolicyProposer(
	delegate Proposer,
	readPolicy func() (*commonpb.ClusterPolicy, error),
	readMembers func() []capabilities.Member,
	businessWrite bool,
) Proposer {
	return CapabilityPolicyProposer{
		delegate:      delegate,
		readPolicy:    readPolicy,
		readMembers:   readMembers,
		businessWrite: businessWrite,
	}
}

func (p CapabilityPolicyProposer) Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	policy, err := p.readPolicy()
	if err != nil {
		return nil, fmt.Errorf("reading cluster capability policy before proposal: %w", err)
	}

	state := capabilities.FromPolicy(policy)
	if err := state.Validate(); err != nil {
		return nil, err
	}

	required := proposal.RequiredCapabilities()
	var members []capabilities.Member
	if p.readMembers != nil {
		members = p.readMembers()
	}
	decision := capabilities.Evaluate(state, required, p.businessWrite, members)
	if !decision.Allowed {
		return nil, fmt.Errorf("%w: %s", capabilities.ErrCapabilityUnsupported, decision.Reason)
	}

	return p.delegate.Propose(ctx, proposal)
}
