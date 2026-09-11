package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// validateMetadataAtApply bounds caller metadata before a save or reversal
// mutates state. Admission may have observed a more permissive policy; only
// the committed policy on this scope determines the replicated outcome.
func validateMetadataAtApply(metadata map[string]*commonpb.MetadataValue, ctx *Context) domain.Describable {
	// Empty input stores no metadata and contributes no bytes to the budget.
	if len(metadata) == 0 {
		return nil
	}
	limits := domain.MetadataLimitsFromPolicy(ctx.Scope.GetClusterPolicy())
	if err := limits.ValidateMap(metadata); err != nil {
		return err
	}
	var total uint64
	if ctx.metadataBudget != nil {
		// Input is already included, along with earlier generated output and all
		// other orders' caller metadata. Do not add this map a second time.
		total = ctx.metadataBudget.bytes
	} else {
		// Direct handler callers execute one order.
		total = domain.MetadataMapSize(metadata)
	}

	return limits.ValidateCommandBytes(total)
}

// validateMetadataKeyAtApply bounds bare caller keys against the committed
// policy before a delete or schema change mutates state.
func validateMetadataKeyAtApply(key string, ctx *Context) domain.Describable {
	limits := domain.MetadataLimitsFromPolicy(ctx.Scope.GetClusterPolicy())
	if err := limits.ValidateKey(key); err != nil {
		return err
	}
	// The command budget already includes every caller key and any earlier
	// generated metadata. Direct handler callers execute only this order.
	var total uint64
	if ctx.metadataBudget != nil {
		total = ctx.metadataBudget.bytes
	} else {
		total = uint64(len(key))
	}

	return limits.ValidateCommandBytes(total)
}
