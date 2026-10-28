// Package proposals reads current payloads and historical Raft entries without
// mutating their accepted intent. New producers only populate Proposal.payload.
package proposals

import "github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"

// BatchOrders returns execution orders. The legacy fields are read only when
// the entry predates the payload envelope; a native policy has no orders.
func BatchOrders(p *raftcmdpb.Proposal) []*raftcmdpb.Order {
	if p.GetPayload() != nil {
		return p.GetBatch().GetOrders()
	}

	return p.GetOrders()
}

func TechnicalUpdates(p *raftcmdpb.Proposal) []*raftcmdpb.TechnicalUpdate {
	if p.GetPayload() != nil {
		return p.GetBatch().GetTechnicalUpdates()
	}

	return p.GetTechnicalUpdates()
}

// IntentCount counts audited operations, including a native policy control.
func IntentCount(p *raftcmdpb.Proposal) int {
	if _, ok := p.GetPayload().(*raftcmdpb.Proposal_SetClusterPolicy); ok {
		return 1
	}

	return len(BatchOrders(p))
}
