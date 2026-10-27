// Package proposals owns the compatibility boundary between historical Raft
// proposals and the proposal payload envelope. Views borrow payloads; they
// never mutate an accepted order or take ownership of pooled protobuf objects.
package proposals

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/attribution"
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

// ValidateShape constrains new producers without narrowing historical replay.
// It does not consult membership, the active policy, or any local setting.
func ValidateShape(p *raftcmdpb.Proposal) error {
	if p == nil {
		return errors.New("nil proposal")
	}
	if p.GetPayload() == nil {
		return nil
	}
	if len(p.GetOrders()) != 0 || len(p.GetTechnicalUpdates()) != 0 {
		return errors.New("proposal mixes legacy fields and payload")
	}
	switch payload := p.GetPayload().(type) {
	case *raftcmdpb.Proposal_Batch:
		orders, updates := payload.Batch.GetOrders(), payload.Batch.GetTechnicalUpdates()
		if len(orders)+len(updates) == 0 {
			return errors.New("empty execution batch")
		}
		var mirrorLedger string
		for i, order := range orders {
			if order == nil {
				return errors.New("nil batch order")
			}
			if _, policy := order.GetSystemScoped().GetPayload().(*raftcmdpb.SystemScopedOrder_SetClusterPolicy); policy {
				return errors.New("cluster policy must use the proposal control")
			}
			if len(updates) != 0 {
				ledger := order.GetLedgerScoped()
				if i == 0 {
					mirrorLedger = ledger.GetLedger()
				}
				if ledger.GetMirrorIngest() == nil || ledger.GetLedger() != mirrorLedger {
					return errors.New("technical updates may only accompany mirror ingestion of the same ledger")
				}
			}
		}
		for _, update := range updates {
			if update == nil || update.GetKind() == nil {
				return errors.New("missing technical update kind")
			}
			if len(orders) == 0 {
				continue
			}
			mirror := update.GetMirrorSync()
			if mirror == nil {
				return errors.New("only mirror sync may accompany orders")
			}
			if mirror.GetLedgerName() != mirrorLedger {
				return errors.New("mirror sync must accompany ingestion of the same ledger")
			}
		}
	case *raftcmdpb.Proposal_SetClusterPolicy:
		if payload.SetClusterPolicy == nil {
			return errors.New("missing policy control")
		}
		if p.GetIdempotency() != nil || p.GetSignature() != nil {
			return errors.New("policy control cannot carry client idempotency or signature")
		}
		if p.GetCallerSnapshot().GetSystem().GetComponent() != string(attribution.ComponentClusterPolicy) {
			return errors.New("policy control requires cluster-policy caller")
		}
	default:
		return fmt.Errorf("unsupported proposal payload %T", payload)
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

// ExecutionView adapts an already validated envelope to the existing audited
// execution pipeline. Legacy input is returned unchanged. New input borrows
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

// LegacyWire validates a logical proposal and lowers it to the released wire
// representation. No producer emits payload-only entries during this expansion
// stage: old and new replicas must execute the same committed bytes. The view
// borrows payload objects so coverage targets survive plan rebuilds.
func LegacyWire(p *raftcmdpb.Proposal) (*raftcmdpb.Proposal, error) {
	if err := ValidateShape(p); err != nil {
		return nil, err
	}

	return ExecutionView(p), nil
}
