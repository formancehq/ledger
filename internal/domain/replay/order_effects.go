package replay

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// OrderEffects captures the boundary-relevant facts that live only on the
// order — not on the ledger-log stream — and therefore must be decoded from
// AuditItem.serialized_order (chain-bound, shipped by the incremental
// export's auditItem segments):
//
//   - MirrorFillGap's skipped transaction ids advance NextTransactionId on
//     the live apply, but FilledGapLog keeps only the original v2 id.
//   - NumscriptExecutionCount increments for script-sourced transactions,
//     but CreatedTransaction logs record the resulting postings, not the
//     content source.
//
// Consumed by backup.RebuildDelta's applyAuditOrderEffects and the checker's
// boundary re-derivation.
type OrderEffects struct {
	Ledger                string
	SkippedTransactionIDs []uint64
	// IsNumscript is exact for successful orders: admission enforces
	// postings XOR script/reference (validateOrderContent), and an
	// unresolvable or empty script reference fails at the FSM without
	// producing a log — so a logged order carrying either script form
	// executed the numscript path.
	IsNumscript bool
}

// DecodeOrderEffects unmarshals a serialized order and extracts its boundary
// effects. Orders with no boundary effect (system-scoped, non-fill-gap mirror
// entries, postings-sourced transactions, metadata orders, ...) return the
// zero OrderEffects with an empty Ledger.
func DecodeOrderEffects(serializedOrder []byte) (OrderEffects, error) {
	order := &raftcmdpb.Order{}
	if err := order.UnmarshalVT(serializedOrder); err != nil {
		return OrderEffects{}, fmt.Errorf("unmarshaling serialized order: %w", err)
	}

	ls := order.GetLedgerScoped()
	if ls == nil {
		return OrderEffects{}, nil
	}

	fillGap := ls.GetMirrorIngest().GetEntry().GetFillGap()
	ct := ls.GetApply().GetCreateTransaction()
	isNumscript := ct != nil && (ct.GetScript().GetPlain() != "" || ct.GetNumscriptReference().GetName() != "")

	if fillGap == nil && !isNumscript {
		return OrderEffects{}, nil
	}

	return OrderEffects{
		Ledger:                ls.GetLedger(),
		SkippedTransactionIDs: fillGap.GetSkippedTransactionIds(),
		IsNumscript:           isNumscript,
	}, nil
}
