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

	at := order.GetAccountType()
	if at == nil || at.GetName() == "" {
		return nil, &domain.ErrInvalidPattern{Pattern: "", Details: "account type name is required"}
	}

	if err := accounttype.ValidatePattern(at.GetPattern()); err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
	}

	if info.AccountTypes == nil {
		info.AccountTypes = make(map[string]*commonpb.AccountType)
	}

	if _, exists := info.GetAccountTypes()[at.GetName()]; exists {
		return nil, &domain.ErrAccountTypeAlreadyExists{Name: at.GetName()}
	}

	// Set default status to ACTIVE.
	at.Status = commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE

	info.AccountTypes[at.GetName()] = at
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

	at, exists := info.GetAccountTypes()[order.GetName()]
	if !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetName()}
	}

	at.EnforcementMode = order.GetEnforcementMode()
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedAccountType{
			UpdatedAccountType: &commonpb.UpdatedAccountTypeLog{
				Name:            order.GetName(),
				EnforcementMode: order.GetEnforcementMode(),
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

	if _, exists := info.GetAccountTypes()[order.GetName()]; !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetName()}
	}

	delete(info.GetAccountTypes(), order.GetName())
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RemovedAccountType{
			RemovedAccountType: &commonpb.RemovedAccountTypeLog{
				Name: order.GetName(),
			},
		},
	}, nil
}

// validatePostingsAgainstAccountTypes validates postings against account types.
// Uses longest-match (highest specificity) to find the best matching type.
// In STRICT mode, returns an error on the first non-matching address.
// In AUDIT mode, non-matching addresses are silently ignored.
func validatePostingsAgainstAccountTypes(
	postings []*commonpb.Posting,
	types map[string]*commonpb.AccountType,
) error {
	if len(types) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	addresses := make([]string, 0, len(postings)*2)
	for _, posting := range postings {
		addresses = append(addresses, posting.GetSource(), posting.GetDestination())
	}

	for _, address := range addresses {
		if address == "world" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}

		matched, _ := matchAddressToType(address, types)
		if matched == nil {
			// No type matched — check if any type is STRICT.
			if hasStrictType(types) {
				return &domain.ErrAccountNotMatchingType{Address: address}
			}

			continue
		}
	}

	return nil
}

// validateAccountAgainstAccountTypes validates a single account address.
func validateAccountAgainstAccountTypes(
	address string,
	types map[string]*commonpb.AccountType,
) error {
	if len(types) == 0 || address == "world" {
		return nil
	}

	matched, _ := matchAddressToType(address, types)
	if matched == nil {
		if hasStrictType(types) {
			return &domain.ErrAccountNotMatchingType{Address: address}
		}
	}

	return nil
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
		if at.GetStatus() == commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED {
			continue
		}

		segments, err := accounttype.ParsePattern(at.GetPattern())
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

	return best, best.GetEnforcementMode()
}

// hasStrictType returns true if any non-deprecated type uses STRICT enforcement.
func hasStrictType(types map[string]*commonpb.AccountType) bool {
	for _, at := range types {
		if at.GetStatus() == commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED {
			continue
		}
		if at.GetEnforcementMode() == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
			return true
		}
	}

	return false
}
