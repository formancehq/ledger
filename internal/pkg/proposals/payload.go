// Package proposals owns the compatibility boundary between historical Raft
// proposals and the proposal payload envelope. Views borrow payloads; they
// never mutate an accepted order or take ownership of pooled protobuf objects.
package proposals

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// BatchOrders returns execution orders, excluding the policy control. Audit
// compatibility is handled separately by ExecutionView.
func BatchOrders(p *raftcmdpb.Proposal) []*raftcmdpb.Order {
	if batch, ok := p.GetPayload().(*raftcmdpb.Proposal_Batch); ok {
		return batch.Batch.GetOrders()
	}

	return p.GetOrders()
}

func TechnicalUpdates(p *raftcmdpb.Proposal) []*raftcmdpb.TechnicalUpdate {
	if batch, ok := p.GetPayload().(*raftcmdpb.Proposal_Batch); ok {
		return batch.Batch.GetTechnicalUpdates()
	}

	return p.GetTechnicalUpdates()
}

// CheckLegacyFields rejects ambiguous proposals before adaptation can discard
// legacy fields. Payload construction belongs to the internal producers.
// TODO: Remove this compatibility check after the next RC.
func CheckLegacyFields(p *raftcmdpb.Proposal) error {
	if p.GetPayload() != nil && (len(p.GetOrders()) != 0 || len(p.GetTechnicalUpdates()) != 0) {
		return errors.New("proposal mixes legacy fields and payload")
	}

	return nil
}

// header creates a borrowed view without copying protobuf runtime state or
// payload buffers. Keeping field assignment explicit avoids copying proto locks.
func header(p *raftcmdpb.Proposal) *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Id: p.GetId(), Date: p.GetDate(), ExecutionPlan: p.GetExecutionPlan(),
		PredictedIndex: p.GetPredictedIndex(), CallerSnapshot: p.GetCallerSnapshot(),
		Idempotency: p.GetIdempotency(), Signature: p.GetSignature(),
	}
}

// PolicyOrder is the historical audit intent for a policy control. Both wire
// forms execute the same policy processor and produce the same log and audit
// bytes. This object is never installed into the received protobuf message.
func PolicyOrder(policy *commonpb.ClusterPolicy) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
		SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy},
		}},
	}}
}

// ExecutionView adapts an envelope checked by CheckLegacyFields to the existing
// audited execution pipeline. Legacy input is returned unchanged. New input borrows
// the payload and header and must not be returned to a protobuf pool.
func ExecutionView(p *raftcmdpb.Proposal) *raftcmdpb.Proposal {
	if p.GetPayload() == nil {
		return p
	}
	view := header(p)
	if policy, ok := p.GetPayload().(*raftcmdpb.Proposal_SetClusterPolicy); ok {
		view.Orders = []*raftcmdpb.Order{PolicyOrder(policy.SetClusterPolicy.GetPolicy())}
	} else {
		view.Orders = BatchOrders(p)
		view.TechnicalUpdates = TechnicalUpdates(p)
	}

	return view
}

// LegacyWire rejects mixed encodings and lowers a proposal to the released wire
// representation. No producer emits payload-only entries during this expansion
// stage: old and new replicas must execute the same committed bytes. The view
// borrows payload objects so coverage targets survive plan rebuilds.
func LegacyWire(p *raftcmdpb.Proposal) (*raftcmdpb.Proposal, error) {
	if err := CheckLegacyFields(p); err != nil {
		return nil, err
	}

	return ExecutionView(p), nil
}
