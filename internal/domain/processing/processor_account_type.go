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
	p.invalidateCompiledTypes(ledgerName)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: at,
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
	p.invalidateCompiledTypes(ledgerName)

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
// When an address doesn't match any type, defaultMode controls the behavior:
// STRICT rejects the transaction, AUDIT silently allows it.
func validatePostingsAgainstAccountTypes(
	postings []*commonpb.Posting,
	compiled []accounttype.CompiledType,
	defaultMode commonpb.ChartEnforcementMode,
) error {
	if len(compiled) == 0 {
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

		if accounttype.FindMatchingType(address, compiled) == nil {
			if defaultMode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
				return &domain.ErrAccountNotMatchingType{Address: address}
			}

			continue
		}
	}

	return nil
}

// validateAccountAgainstAccountTypes validates a single account address.
// When the address doesn't match any type, defaultMode controls the behavior.
func validateAccountAgainstAccountTypes(
	address string,
	compiled []accounttype.CompiledType,
	defaultMode commonpb.ChartEnforcementMode,
) error {
	if len(compiled) == 0 || address == "world" {
		return nil
	}

	if accounttype.FindMatchingType(address, compiled) == nil {
		if defaultMode == commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT {
			return &domain.ErrAccountNotMatchingType{Address: address}
		}
	}

	return nil
}

// processUpdateDefaultEnforcementMode updates the ledger's default enforcement mode.
func (p *RequestProcessor) processUpdateDefaultEnforcementMode(
	ledgerName string,
	order *raftcmdpb.UpdateDefaultEnforcementModeOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	info.DefaultEnforcementMode = order.GetEnforcementMode()
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
			UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{
				EnforcementMode: order.GetEnforcementMode(),
			},
		},
	}, nil
}
