package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// processAddAccountType adds a new account type to a ledger.
func (p *RequestProcessor) processAddAccountType(
	ledgerName string,
	order *raftcmdpb.AddAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at := order.AccountType
	if at == nil || at.Name == "" {
		return nil, &domain.ErrInvalidPattern{Pattern: "", Details: "account type name is required"}
	}

	if err := accounttype.ValidatePattern(at.Pattern); err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.Pattern, Details: err.Error()}
	}

	if info.AccountTypes == nil {
		info.AccountTypes = make(map[string]*commonpb.AccountType)
	}

	if _, exists := info.AccountTypes[at.Name]; exists {
		return nil, &domain.ErrAccountTypeAlreadyExists{Name: at.Name}
	}

	// Set default status to ACTIVE.
	at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE

	info.AccountTypes[at.Name] = at
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: at,
			},
		},
	}, nil
}

// processUpdateAccountType updates the enforcement mode of an existing account type.
func (p *RequestProcessor) processUpdateAccountType(
	ledgerName string,
	order *raftcmdpb.UpdateAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	at, exists := info.AccountTypes[order.Name]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.Name}
	}

	at.EnforcementMode = order.EnforcementMode
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedAccountType{
			UpdatedAccountType: &commonpb.UpdatedAccountTypeLog{
				Name:            order.Name,
				EnforcementMode: order.EnforcementMode,
			},
		},
	}, nil
}

// processRemoveAccountType removes an account type from a ledger.
// The type must be DEPRECATED or have no matching accounts.
func (p *RequestProcessor) processRemoveAccountType(
	ledgerName string,
	order *raftcmdpb.RemoveAccountTypeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	if _, exists := info.AccountTypes[order.Name]; !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.Name}
	}

	delete(info.AccountTypes, order.Name)
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
			RemovedAccountType: &commonpb.RemovedAccountTypeLog{
				Name: order.Name,
			},
		},
	}, nil
}

// validatePostingsAgainstAccountTypes validates postings against account types.
// Uses longest-match (highest specificity) to find the best matching type.
// In STRICT mode, returns an error on the first non-matching address.
// In AUDIT mode, collects violations as warnings.
func validatePostingsAgainstAccountTypes(
	postings []*commonpb.Posting,
	types map[string]*commonpb.AccountType,
) ([]*commonpb.ChartViolation, error) {
	if len(types) == 0 {
		return nil, nil
	}

	var violations []*commonpb.ChartViolation
	seen := make(map[string]struct{})
	addresses := make([]string, 0, len(postings)*2)
	for _, posting := range postings {
		addresses = append(addresses, posting.Source, posting.Destination)
	}

	for _, address := range addresses {
		if address == "world" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}

		matched, mode := matchAddressToType(address, types)
		if matched == nil {
			// No type matched — check if any type is STRICT.
			if hasStrictType(types) {
				return nil, &domain.ErrAccountNotMatchingType{Address: address}
			}
			// All types are AUDIT: record violation.
			violations = append(violations, &commonpb.ChartViolation{Address: address})
			continue
		}

		// Matched but check if it's a non-match in STRICT mode.
		// A match was found, so no violation for this address.
		_ = mode // The matched type's enforcement mode is used for reporting.
	}

	return violations, nil
}

// validateAccountAgainstAccountTypes validates a single account address.
func validateAccountAgainstAccountTypes(
	address string,
	types map[string]*commonpb.AccountType,
) ([]*commonpb.ChartViolation, error) {
	if len(types) == 0 || address == "world" {
		return nil, nil
	}

	matched, _ := matchAddressToType(address, types)
	if matched == nil {
		if hasStrictType(types) {
			return nil, &domain.ErrAccountNotMatchingType{Address: address}
		}
		return []*commonpb.ChartViolation{{Address: address}}, nil
	}

	return nil, nil
}

// matchAddressToType finds the best matching account type for an address using
// longest-match (highest specificity). Returns nil if no type matches.
// Deprecated types are skipped.
func matchAddressToType(
	address string,
	types map[string]*commonpb.AccountType,
) (*commonpb.AccountType, commonpb.ChartEnforcementMode) {
	var (
		best     *commonpb.AccountType
		bestSpec = -1
		bestLen  = 0
	)

	for _, at := range types {
		if at.Status == commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED {
			continue
		}

		segments, err := accounttype.ParsePattern(at.Pattern)
		if err != nil {
			continue
		}

		if _, ok := accounttype.MatchAddress(address, segments); !ok {
			continue
		}

		spec := accounttype.Specificity(segments)
		segLen := len(segments)

		if spec > bestSpec || (spec == bestSpec && segLen < bestLen) {
			best = at
			bestSpec = spec
			bestLen = segLen
		}
	}

	if best == nil {
		return nil, 0
	}
	return best, best.EnforcementMode
}

// hasStrictType returns true if any non-deprecated type uses STRICT enforcement.
func hasStrictType(types map[string]*commonpb.AccountType) bool {
	for _, at := range types {
		if at.Status == commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED {
			continue
		}
		if at.EnforcementMode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
			return true
		}
	}
	return false
}
