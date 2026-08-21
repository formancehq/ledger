package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processSetClusterPolicy applies a cluster-policy update after the idempotency
// gate. The revision is validated against the applied policy so a committed
// entry resolves identically on every node:
//
//   - higher revision: apply and emit the audit log;
//   - same revision, identical payload: idempotent no-op (no log);
//   - same revision, different payload: contract violation, rejected;
//   - lower revision: stale, rejected (a newer policy already won).
func processSetClusterPolicy(order *raftcmdpb.SetClusterPolicyOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	newPolicy := order.GetPolicy()
	if newPolicy == nil {
		return nil, &domain.ErrClusterPolicyInvalid{Detail: "missing policy"}
	}

	if newPolicy.GetRevision() == 0 {
		return nil, &domain.ErrClusterPolicyInvalid{Detail: "revision must be at least 1"}
	}

	if newPolicy.GetQueryCheckpointLimit() < 1 {
		return nil, &domain.ErrClusterPolicyInvalid{Detail: "query_checkpoint_limit must be at least 1"}
	}

	current := ctx.Scope.GetClusterPolicy()
	appliedRev := current.GetRevision()
	newRev := newPolicy.GetRevision()

	switch {
	case newRev > appliedRev:
		ctx.Scope.SetClusterPolicy(newPolicy)

		return &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetClusterPolicy{
				SetClusterPolicy: &commonpb.SetClusterPolicyLog{Policy: newPolicy},
			},
		}, nil
	case newRev < appliedRev:
		return nil, &domain.ErrStaleClusterPolicy{ProposedRevision: newRev, AppliedRevision: appliedRev}
	default:
		if current.EqualVT(newPolicy) {
			return nil, nil
		}

		return nil, &domain.ErrClusterPolicyRevisionConflict{Revision: newRev}
	}
}
