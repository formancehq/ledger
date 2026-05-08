package bootstrap

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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
	cmd := commands.NewCommand(orders...)

	data, err := cmd.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling proposal: %w", err)
	}

	proposal := node.NewProposal(cmd.GetId(), data)

	// Lock the tracker to serialize the Increment with guarded proposals,
	// preventing preload boundary mismatches in the FSM.
	p.node.IndexTracker().Lock()
	_, err = p.node.Propose(context.Background(), proposal)
	p.node.IndexTracker().Unlock()

	if err != nil {
		return fmt.Errorf("proposing to raft: %w", err)
	}

	// Wait for the raw Raft proposal to be accepted (not FSM application).
	if _, err := proposal.Wait(); err != nil {
		return fmt.Errorf("raft proposal failed: %w", err)
	}

	return nil
}

// ProposeIdempotencyEviction proposes a deterministic idempotency eviction
// command through Raft. All nodes apply the same cutoff, removing expired keys.
func (p *NodeProposer) ProposeIdempotencyEviction(cutoffMicros uint64) error {
	cmd := &raftcmdpb.Proposal{
		Id:                  rand.Uint64(),
		Date:                &commonpb.Timestamp{Data: uint64(time.Now().UnixMicro())},
		IdempotencyEviction: &raftcmdpb.IdempotencyEviction{CutoffMicros: cutoffMicros},
	}

	data, err := cmd.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling eviction proposal: %w", err)
	}

	proposal := node.NewProposal(cmd.GetId(), data)

	p.node.IndexTracker().Lock()
	_, err = p.node.Propose(context.Background(), proposal)
	p.node.IndexTracker().Unlock()

	if err != nil {
		return fmt.Errorf("proposing eviction to raft: %w", err)
	}

	if _, err := proposal.Wait(); err != nil {
		return fmt.Errorf("eviction proposal failed: %w", err)
	}

	return nil
}
