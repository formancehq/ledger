package node

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

// LockedProposer wraps a Node to acquire the IndexTracker lock around Propose.
// This serializes the tracker Increment with guarded proposals (admission,
// mirror data path), preventing system proposals (events sink, barrier, etc.)
// from inflating the tracker between another proposal's BuildPreloads and
// AcquireProposalGuard — which would cause a preload boundary mismatch in
// the FSM.
type LockedProposer struct {
	node *Node
}

// NewLockedProposer creates a LockedProposer wrapping the given Node.
func NewLockedProposer(n *Node) *LockedProposer {
	return &LockedProposer{node: n}
}

// Propose acquires the IndexTracker lock, proposes to Raft, and releases the
// lock. The Increment inside Node.Propose runs while the lock is held,
// serializing it with the preloader's proposal guard.
func (lp *LockedProposer) Propose(ctx context.Context, proposal *Proposal) (*futures.Future[state.ApplyResult], error) {
	lp.node.indexTracker.Lock()
	defer lp.node.indexTracker.Unlock()

	return lp.node.Propose(ctx, proposal)
}
