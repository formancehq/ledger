package admission

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateOrder validates storage-safety invariants on a fully-constructed order
// before it enters the Raft pipeline. This is the single validation gate for all
// write paths (gRPC, HTTP, bulk).
func validateOrder(order *raftcmdpb.Order) error {
	if err := validateOrderLedgerName(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderMetadata(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderAccountAddresses(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	if err := validateOrderContent(order); err != nil {
		return &domain.BusinessError{Err: err}
	}

	return nil
}

// validateOrderLedgerName extracts and validates the ledger name from any order type.
func validateOrderLedgerName(order *raftcmdpb.Order) domain.Describable {
	var name string

	switch o := order.GetType().(type) {
	case *raftcmdpb.Order_CreateLedger:
		name = o.CreateLedger.GetName()
	case *raftcmdpb.Order_DeleteLedger:
		name = o.DeleteLedger.GetName()
	case *raftcmdpb.Order_Apply:
		name = o.Apply.GetLedger()
	case *raftcmdpb.Order_SaveNumscript:
		name = o.SaveNumscript.GetLedger()
	case *raftcmdpb.Order_DeleteNumscript:
		name = o.DeleteNumscript.GetLedger()
	case *raftcmdpb.Order_PromoteLedger:
		name = o.PromoteLedger.GetLedger()
	case *raftcmdpb.Order_SaveLedgerMetadata:
		name = o.SaveLedgerMetadata.GetLedger()
	case *raftcmdpb.Order_DeleteLedgerMetadata:
		name = o.DeleteLedgerMetadata.GetLedger()
	case *raftcmdpb.Order_UpdatePreparedQuery:
		name = o.UpdatePreparedQuery.GetLedger()
	case *raftcmdpb.Order_DeletePreparedQuery:
		name = o.DeletePreparedQuery.GetLedger()
	case *raftcmdpb.Order_CreatePreparedQuery:
		name = o.CreatePreparedQuery.GetQuery().GetLedger()
	case *raftcmdpb.Order_MirrorIngest:
		name = o.MirrorIngest.GetLedger()
	default:
		return nil
	}

	return domain.ValidateLedgerName(name)
}

// validateOrderMetadata validates that all metadata keys and values in the order
// are safe for Pebble key encoding.
func validateOrderMetadata(order *raftcmdpb.Order) domain.Describable {
	switch o := order.GetType().(type) {
	case *raftcmdpb.Order_Apply:
		return validateApplyMetadata(o.Apply)
	case *raftcmdpb.Order_SaveLedgerMetadata:
		return validateMetadataMap(o.SaveLedgerMetadata.GetMetadata())
	case *raftcmdpb.Order_DeleteLedgerMetadata:
		return domain.ValidateMetadataKey(o.DeleteLedgerMetadata.GetKey())
	case *raftcmdpb.Order_MirrorIngest:
		return validateMirrorMetadata(o.MirrorIngest.GetEntry())
	default:
		return nil
	}
}

// validateApplyMetadata validates metadata within a LedgerApplyOrder.
func validateApplyMetadata(apply *raftcmdpb.LedgerApplyOrder) domain.Describable {
	switch d := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		if err := validateMetadataMap(d.CreateTransaction.GetMetadata()); err != nil {
			return err
		}

		for account, mm := range d.CreateTransaction.GetAccountMetadata() {
			if mm != nil {
				if err := validateMetadataMap(mm.GetValues()); err != nil {
					return &domain.ErrAccountValidation{Account: account, Cause: err}
				}
			}
		}

		return nil
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		// processRevertTransaction stores order.GetMetadata() straight into
		// the revert log payload, so the metadata-key invariants (non-empty,
		// no NUL bytes) must be checked here too. Without this gate a
		// client-supplied empty or NUL-bearing key reaches the canonical
		// Pebble key layout via the revert log and corrupts read-index
		// entries (#322).
		return validateMetadataMap(d.RevertTransaction.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		return validateMetadataMap(d.AddMetadata.GetMetadata())
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		return domain.ValidateMetadataKey(d.DeleteMetadata.GetKey())
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		return domain.ValidateMetadataKey(d.SetMetadataFieldType.GetKey())
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		return domain.ValidateMetadataKey(d.RemoveMetadataFieldType.GetKey())
	default:
		return nil
	}
}

// validateMirrorMetadata validates metadata supplied by mirror ingest orders.
func validateMirrorMetadata(entry *raftcmdpb.MirrorLogEntry) domain.Describable {
	switch d := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		if err := validateMetadataMap(d.CreatedTransaction.GetMetadata()); err != nil {
			return err
		}

		for account, mm := range d.CreatedTransaction.GetAccountMetadata() {
			if mm != nil {
				if err := validateMetadataMap(mm.GetValues()); err != nil {
					return &domain.ErrAccountValidation{Account: account, Cause: err}
				}
			}
		}

		return nil
	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		return validateMetadataMap(d.SavedMetadata.GetMetadata())
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		return validateMetadataMap(d.RevertedTransaction.GetMetadata())
	default:
		return nil
	}
}

// validateOrderAccountAddresses validates account addresses in non-transaction orders
// (metadata targets). Transaction postings are validated in the processor after
// Numscript resolution.
func validateOrderAccountAddresses(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetType().(*raftcmdpb.Order_Apply)
	if !ok {
		return nil
	}

	switch d := apply.Apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		if t := d.AddMetadata.GetTarget().GetAccount(); t != nil {
			return domain.ValidateAccountAddress(t.GetAddr())
		}

		return nil
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		if t := d.DeleteMetadata.GetTarget().GetAccount(); t != nil {
			return domain.ValidateAccountAddress(t.GetAddr())
		}

		return nil
	default:
		return nil
	}
}

// validateOrderContent enforces structural well-formedness on the order
// payload (currently CreateTransaction). It rejects orders that declare no
// content source (no postings, no inline script, no script reference) and
// orders that combine explicit postings with a script — both shapes silently
// produce an unintended transaction at the FSM (#452).
//
// The "result must contain ≥1 posting" invariant — needed to catch numscripts
// that execute fine but emit no postings — lives on the FSM
// (processCreateTransaction) because only it sees the post-producer result.
func validateOrderContent(order *raftcmdpb.Order) domain.Describable {
	apply, ok := order.GetType().(*raftcmdpb.Order_Apply)
	if !ok {
		return nil
	}

	ct, ok := apply.Apply.GetData().(*raftcmdpb.LedgerApplyOrder_CreateTransaction)
	if !ok {
		return nil
	}

	o := ct.CreateTransaction
	hasPostings := len(o.GetPostings()) > 0
	hasInlineScript := o.GetScript() != nil && o.GetScript().GetPlain() != ""
	// Two booleans on the reference because the two gates have different needs:
	//   - refPresent drives the conflict check: any reference (even nameless)
	//     signals the caller intended the script path, so postings alongside is
	//     ambiguous and must be rejected.
	//   - refValid drives the empty-payload check: a nameless reference is not
	//     real content (the FSM would surface ErrNumscriptNotFound{Name:""}),
	//     so an order with only `scriptReference: {}` is empty.
	refPresent := o.GetNumscriptReference() != nil
	refValid := refPresent && o.GetNumscriptReference().GetName() != ""

	switch {
	case hasPostings && (hasInlineScript || refPresent):
		return domain.ErrPostingsAndScriptConflict
	case !hasPostings && !hasInlineScript && !refValid:
		return domain.ErrEmptyTransaction
	}

	return nil
}

// validateMetadataMap validates all keys and values in a metadata map.
// Value-level failures are wrapped in ErrMetadataKeyValidation so the
// offending key reaches operator logs and the gRPC ErrorInfo metadata
// (rather than being dropped, which the first pass of this refactor did
// before paul-nicolas's review).
func validateMetadataMap(m map[string]*commonpb.MetadataValue) domain.Describable {
	for key, value := range m {
		if err := domain.ValidateMetadataKey(key); err != nil {
			return err
		}

		if err := domain.ValidateMetadataValue(value); err != nil {
			return &domain.ErrMetadataKeyValidation{Key: key, Cause: err}
		}
	}

	return nil
}
