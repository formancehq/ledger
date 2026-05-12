package bootstrap

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// NodeProposer adapts *node.Node to the state.Proposer interface by serializing
// raftcmdpb.Order objects into a Proposal and submitting them to Raft.
type NodeProposer struct {
	node *node.Node
}

// NewNodeProposer creates a new NodeProposer wrapping the given node.
func NewNodeProposer(n *node.Node) *NodeProposer {
	return &NodeProposer{node: n}
}

// ProposeOrders builds a raftcmdpb.Proposal from the given orders, serializes
// it, and proposes it to the Raft cluster. It waits for the Raft proposal to
// be accepted but does not wait for FSM application.
func (p *NodeProposer) ProposeOrders(orders ...*raftcmdpb.Order) error {
	return p.propose(commands.NewCommand(orders...))
}

// ProposeProposal submits a pre-built Proposal to the Raft cluster.
// Used for technical proposals (e.g. cluster config updates) that don't
// carry orders or produce log entries.
func (p *NodeProposer) ProposeProposal(cmd *raftcmdpb.Proposal) error {
	return p.propose(cmd)
}

func (p *NodeProposer) propose(cmd *raftcmdpb.Proposal) error {
	data, err := cmd.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling proposal: %w", err)
	}

	proposal := node.NewProposal(cmd.GetId(), data)

	p.node.IndexTracker().Lock()
	_, err = p.node.Propose(context.Background(), proposal)
	p.node.IndexTracker().Unlock()

	if err != nil {
		return fmt.Errorf("proposing to raft: %w", err)
	}

	if _, err := proposal.Wait(); err != nil {
		return fmt.Errorf("raft proposal failed: %w", err)
	}

	return nil
}
