package plan

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CapabilityPolicyProposer rechecks the committed capability policy against
// the final serialized proposal immediately before handing it to the Raft
// proposer. Technical producers use businessWrite=false: ALL_WRITES blocks
// business traffic while retaining the control-plane recovery path.
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

	final := new(raftcmdpb.Proposal)
	if err := final.UnmarshalVT(proposal.Data()); err != nil {
		return nil, fmt.Errorf("decode proposal for capability admission: %w", err)
	}

	required := make([]capabilities.ID, len(final.GetRequiredCapabilities()))
	for i, id := range final.GetRequiredCapabilities() {
		required[i] = capabilities.ID(id)
	}
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
