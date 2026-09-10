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

	// The metadata ceilings gate business writes at admission AND inside apply
	// (Numscript-produced metadata), so a committed policy must always carry
	// usable numbers. A zero ceiling is the absence of configuration, never
	// "unlimited": committing it would silently disable the protection, so it is
	// refused here rather than repaired with a default — a node-local default
	// would also make apply node-dependent (invariant #2).
	if err := validateClusterPolicyMetadataLimits(newPolicy); err != nil {
		return nil, err
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

// validateClusterPolicyMetadataLimits rejects a policy whose metadata ceilings
// are unusable. Each ceiling must be at least 1, and the ceilings must be
// mutually satisfiable: a per-value ceiling above the per-entity one — or a
// per-entity ceiling above the per-command one — is unreachable, so an operator
// raising it would observe no effect. Both checks are pure functions of the
// proposed policy, so every node reaches the same verdict for one committed
// entry.
func validateClusterPolicyMetadataLimits(policy *commonpb.ClusterPolicy) domain.Describable {
	limits := domain.MetadataLimitsFromPolicy(policy)

	if !limits.Configured() {
		return &domain.ErrClusterPolicyInvalid{
			Detail: "every metadata_max_* limit must be at least 1",
		}
	}

	if !limits.Consistent() {
		return &domain.ErrClusterPolicyInvalid{
			Detail: "metadata limits must satisfy metadata_max_key_bytes <= metadata_max_entity_bytes, " +
				"metadata_max_value_bytes <= metadata_max_entity_bytes and " +
				"metadata_max_entity_bytes <= metadata_max_command_bytes",
		}
	}

	return nil
}
