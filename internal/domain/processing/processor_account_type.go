package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processAddAccountType adds a new account type to a ledger.
func processAddAccountType(ledger string, order *raftcmdpb.AddAccountTypeOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(ctx.Scope, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	at := order.GetAccountType()
	if at == nil || at.GetName() == "" {
		return nil, &domain.ErrInvalidPattern{Pattern: "", Details: "account type name is required"}
	}

	newSegments, err := accounttype.ParsePattern(at.GetPattern())
	if err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
	}

	if err := accounttype.ValidateSegmentTypes(newSegments, at.GetSegmentTypes()); err != nil {
		return nil, &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: err.Error()}
	}

	if err := validateDefaultMetadata(at); err != nil {
		return nil, err
	}

	if info.AccountTypes == nil {
		info.AccountTypes = make(map[string]*commonpb.AccountType)
	}

	if _, exists := info.GetAccountTypes()[at.GetName()]; exists {
		return nil, &domain.ErrAccountTypeAlreadyExists{Name: at.GetName()}
	}

	// Check for conflicts with existing account types.
	for existingName, existing := range info.GetAccountTypes() {
		existingSegments, parseErr := accounttype.ParsePattern(existing.GetPattern())
		if parseErr != nil {
			continue
		}

		if accounttype.PatternsConflict(newSegments, existingSegments) {
			return nil, &domain.ErrAccountTypeConflict{
				NewPattern:      at.GetPattern(),
				ExistingName:    existingName,
				ExistingPattern: existing.GetPattern(),
			}
		}
	}

	info.AccountTypes[at.GetName()] = at
	ctx.Scope.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)
	invalidateCompiledTypes(ctx.CompiledTypes, ledger)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: at,
			},
		},
	}, nil
}

// validateDefaultMetadata checks an account type's default_metadata is
// structurally sound: non-empty keys and non-nil values. Values are stored raw
// and coerced to the declared type at read (immutable-values model, #503), so
// there is no write-time schema coercion to mirror here — this rejects only the
// malformed entries that could never resolve to a usable value.
func validateDefaultMetadata(at *commonpb.AccountType) domain.Describable {
	for key, value := range at.GetDefaultMetadata() {
		if key == "" {
			return &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: "default_metadata has an empty key"}
		}

		if value == nil {
			return &domain.ErrInvalidPattern{Pattern: at.GetPattern(), Details: "default_metadata key " + key + " has a nil value"}
		}
	}

	return nil
}

// processRemoveAccountType removes an account type from a ledger.
func processRemoveAccountType(ledger string, order *raftcmdpb.RemoveAccountTypeOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(ctx.Scope, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	if _, exists := info.GetAccountTypes()[order.GetName()]; !exists {
		return nil, &domain.ErrAccountTypeNotFound{Name: order.GetName()}
	}

	delete(info.GetAccountTypes(), order.GetName())
	ctx.Scope.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)
	invalidateCompiledTypes(ctx.CompiledTypes, ledger)

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
) domain.Describable {
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
) domain.Describable {
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
func processUpdateDefaultEnforcementMode(ledger string, order *raftcmdpb.UpdateDefaultEnforcementModeOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	info, loadErr := loadLedger(ctx.Scope, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	info = info.CloneVT()

	info.DefaultEnforcementMode = order.GetEnforcementMode()
	ctx.Scope.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{
			UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{
				EnforcementMode: order.GetEnforcementMode(),
			},
		},
	}, nil
}
