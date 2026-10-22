package processing

import (
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
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
func processSetClusterPolicy(order *raftcmdpb.SetClusterPolicyOrder, ctx *Context) (*ledgerpb.LogPayload, domain.SerializableError) {
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
	seenCapabilities := make(map[uint32]struct{}, len(newPolicy.GetCapabilityActivations()))
	for _, activation := range newPolicy.GetCapabilityActivations() {
		if activation == nil || activation.GetId() == 0 {
			return nil, &domain.ErrClusterPolicyInvalid{Detail: "capability activation id must be non-zero"}
		}
		if _, exists := seenCapabilities[activation.GetId()]; exists {
			return nil, &domain.ErrClusterPolicyInvalid{Detail: fmt.Sprintf("duplicate capability activation %d", activation.GetId())}
		}
		seenCapabilities[activation.GetId()] = struct{}{}
	}

	current := scope.GetClusterPolicy()
	appliedRev := current.GetRevision()
	newRev := newPolicy.GetRevision()
	active := make(map[uint32]struct{})
	for _, activation := range current.GetCapabilityActivations() {
		if activation != nil && activation.GetActive() {
			active[activation.GetId()] = struct{}{}
		}
	}
	for id := range active {
		found := false
		for _, activation := range newPolicy.GetCapabilityActivations() {
			if activation != nil && activation.GetId() == id && activation.GetActive() {
				found = true

				break
			}
		}
		if !found {
			return nil, &domain.ErrClusterPolicyInvalid{Detail: fmt.Sprintf("capability %d cannot be deactivated", id)}
		}
	}

	switch {
	case newRev > appliedRev:
		scope.SetClusterPolicy(newPolicy)

		return &ledgerpb.LogPayload{
			Type: &ledgerpb.LogPayload_SetClusterPolicy{
				SetClusterPolicy: &ledgerpb.SetClusterPolicyLog{Policy: newPolicy},
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
func validateClusterPolicyMetadataLimits(policy *ledgerpb.ClusterPolicy) domain.SerializableError {
	if err := domain.MetadataLimitsFromPolicy(policy).Validate(); err != nil {
		return &domain.ErrClusterPolicyInvalid{Detail: err.Error()}
	}

	return nil
}

// ProcessClusterPolicy applies operational state without allocating business logs.
// The released order handler above retains its original audited replay behavior.
func (p *RequestProcessor) ProcessClusterPolicy(proposal *raftcmdpb.SetClusterPolicyProposal, factory ScopeFactory) domain.Describable {
	policy := proposal.GetPolicy()
	if proposal.GetConfigurationOnly() && (policy == nil || len(policy.GetCapabilityActivations()) != 0) {
		return &domain.ErrClusterPolicyInvalid{Detail: "cluster policy cannot contain capabilities"}
	}
	scope, err := factory.NewScope(nil)
	if err != nil {
		return &domain.ErrInvalidExecutionPlan{Reason_: err.Error()}
	}
	if proposal.GetConfigurationOnly() {
		policy = policy.CloneVT()
		policy.CapabilityActivations = scope.GetClusterPolicy().CloneVT().GetCapabilityActivations()
	}
	_, processErr := applyClusterPolicy(policy, scope)

	return processErr
}

func (p *RequestProcessor) ProcessCapabilityTransition(transition *commonpb.CapabilityActivation, factory ScopeFactory) domain.Describable {
	if transition == nil || transition.GetId() == 0 {
		return &domain.ErrClusterPolicyInvalid{Detail: "capability transition requires a non-zero id"}
	}
	if !transition.GetActive() {
		return &domain.ErrClusterPolicyInvalid{Detail: "capability transition must activate"}
	}
	scope, err := factory.NewScope(nil)
	if err != nil {
		return &domain.ErrInvalidExecutionPlan{Reason_: err.Error()}
	}
	desired := scope.GetClusterPolicy().CloneVT()
	for _, current := range desired.GetCapabilityActivations() {
		if current.GetId() != transition.GetId() {
			continue
		}
		if current.GetActive() {
			return nil
		}
		current.Active = true
		scope.SetClusterPolicy(desired)

		return nil
	}
	desired.CapabilityActivations = append(desired.CapabilityActivations, &commonpb.CapabilityActivation{Id: transition.GetId(), Active: true})
	scope.SetClusterPolicy(desired)

	return nil
}
