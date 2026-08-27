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
//   - MirrorIngest's source v2 log id advances LastMirrorV2LogId on the live
//     apply (processing.processMirrorIngest), but only FilledGapLog echoes it
//     back into the ledger-log stream — the other ingest kinds
//     (CreatedTransaction, SavedMetadata, DeletedMetadata, RevertedTransaction)
//     carry it solely on the wrapping MirrorLogEntry.
//
// Consumed by backup.RebuildDelta's applyAuditOrderEffects and the checker's
// boundary re-derivation.
type OrderEffects struct {
	Ledger                string
	SkippedTransactionIDs []uint64
	// MirrorV2LogID is the source v2 log id of a MirrorIngest order, on any
	// ingest kind (it lives on the wrapping MirrorLogEntry). Zero for every
	// non-mirror order. Folded as a max into LedgerBoundaries.
	// LastMirrorV2LogId — never assigned — so the rebuild agrees by
	// construction with the checker's own max fold in
	// recordMirrorIngestMutations (EN-1776).
	MirrorV2LogID uint64
}

// DecodeOrderEffects unmarshals a serialized order and extracts its boundary
// effects. Orders with no boundary effect (system-scoped, postings-sourced
// transactions, metadata orders, ...) return the zero OrderEffects with an
// empty Ledger.
//
// Every MirrorIngest carrying a source v2 log id qualifies, not just the
// fill-gap kind: LastMirrorV2LogId advances on each of them, and
// CreatedTransaction is the common kind. Restricting this to fill-gaps left
// RebuildDelta unable to reconstruct the mirror high-water mark at all, so a
// restored mirror re-applied logs it had already applied (EN-1776).
func DecodeOrderEffects(serializedOrder []byte) (OrderEffects, error) {
	order := &raftcmdpb.Order{}
	if err := order.UnmarshalVT(serializedOrder); err != nil {
		return OrderEffects{}, fmt.Errorf("unmarshaling serialized order: %w", err)
	}

	ls := order.GetLedgerScoped()
	if ls == nil {
		return OrderEffects{}, nil
	}

	entry := ls.GetMirrorIngest().GetEntry()
	fillGap := entry.GetFillGap()
	// v2 log ids are 1-based, so zero means "no mirror ingest here" rather
	// than a legitimate id — matching processMirrorIngest, which rejects a
	// zero id outright instead of recording it.
	mirrorV2LogID := entry.GetV2LogId()

	if fillGap == nil && mirrorV2LogID == 0 {
		return OrderEffects{}, nil
	}

	return OrderEffects{
		Ledger:                ls.GetLedger(),
		SkippedTransactionIDs: fillGap.GetSkippedTransactionIds(),
		MirrorV2LogID:         mirrorV2LogID,
	}, nil
}
