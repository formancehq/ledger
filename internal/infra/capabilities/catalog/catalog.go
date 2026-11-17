// Package catalog owns the capabilities implemented by this Ledger binary and
// their proposal-specific selection rules. The parent capabilities package
// contains only the reusable policy and membership decision contract.
package catalog

import (
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/pkg/proposals"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// CreateLedgerAccountTypesV2 selects the strengthened initial account-type
// validation. An unmarked proposal keeps the released apply semantics.
const CreateLedgerAccountTypesV2 capabilities.CapabilityID = 1

// SupportedIDs is the immutable capability set implemented by this binary.
func SupportedIDs() []capabilities.CapabilityID {
	return []capabilities.CapabilityID{CreateLedgerAccountTypesV2}
}

// HasCreateLedger inspects both current execution batches and historical
// proposals. A proposal-wide marker applies to every CreateLedger it contains.
func HasCreateLedger(proposal *raftcmdpb.Proposal) bool {
	if proposal == nil {
		return false
	}
	for _, order := range proposals.BatchOrders(proposal) {
		if order.GetLedgerScoped().GetCreateLedger() != nil {
			return true
		}
	}

	return false
}

// SelectCreateLedgerValidation stamps the proposal before serialization when
// admission observes an active policy. The final Node.Propose gate rechecks
// policy so an activation racing this read rejects an unmarked proposal.
func SelectCreateLedgerValidation(proposal *raftcmdpb.Proposal, policy *commonpb.ClusterPolicy) {
	if HasCreateLedger(proposal) && capabilities.FromPolicy(policy).Activations[CreateLedgerAccountTypesV2] {
		proposal.RequiredCapabilities = []uint32{uint32(CreateLedgerAccountTypesV2)}
	}
}

// ValidateCreateLedgerDeclaration enforces activation at the final admission
// boundary, before the proposal receives a Raft index.
func ValidateCreateLedgerDeclaration(policy *commonpb.ClusterPolicy, hasCreateLedger bool, required []capabilities.CapabilityID) error {
	if hasCreateLedger && capabilities.FromPolicy(policy).Activations[CreateLedgerAccountTypesV2] && !slices.Contains(required, CreateLedgerAccountTypesV2) {
		return fmt.Errorf("%w: CreateLedger requires capability %d", capabilities.ErrMissingCapability, CreateLedgerAccountTypesV2)
	}

	return nil
}

// ValidateDeclaration checks the explicit proposal marker against this
// binary's catalog. It does not derive the marker: the same CreateLedger
// payload legitimately has old or new semantics depending on that marker.
func ValidateDeclaration(proposal *raftcmdpb.Proposal) ([]capabilities.CapabilityID, error) {
	if proposal == nil {
		return nil, fmt.Errorf("%w: nil proposal", capabilities.ErrInvalidDeclaration)
	}
	ids := proposal.GetRequiredCapabilities()
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) != 1 || capabilities.CapabilityID(ids[0]) != CreateLedgerAccountTypesV2 || !HasCreateLedger(proposal) {
		return nil, fmt.Errorf("%w: capability declaration does not match proposal orders", capabilities.ErrInvalidDeclaration)
	}

	return []capabilities.CapabilityID{CreateLedgerAccountTypesV2}, nil
}

// ValidateSerializedDeclaration checks that the final bytes carry the same
// validated marker as the typed metadata used at admission. The predicted
// index append may not change this declaration.
func ValidateSerializedDeclaration(data []byte, declared []capabilities.CapabilityID) error {
	proposal := new(raftcmdpb.Proposal)
	if err := proposal.UnmarshalVT(data); err != nil {
		return fmt.Errorf("decode final proposal: %w", err)
	}
	wire, err := ValidateDeclaration(proposal)
	if err != nil {
		return err
	}
	if !slices.Equal(wire, declared) {
		return fmt.Errorf("%w: declaration does not match final payload", capabilities.ErrMissingCapability)
	}

	return nil
}
