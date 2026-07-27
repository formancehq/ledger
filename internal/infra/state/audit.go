package state

import (
	"errors"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// buildAuditFailure extracts the error type and context from a processing error
// to build an AuditFailure proto. Previously this was a 30-case hand-maintained
// type-switch that would fall to "UNKNOWN" on every new typed error; it now
// consumes the Describable contract directly so adding a new domain error type
// automatically extends audit coverage with no edit here.
func buildAuditFailure(err error) *auditpb.AuditFailure {
	failure := &auditpb.AuditFailure{
		Message: err.Error(),
		Context: make(map[string]string),
	}

	var d domain.Describable
	if errors.As(err, &d) {
		failure.Reason = domain.ReasonCode(d.Reason())

		maps.Copy(failure.GetContext(), d.Metadata())

		return failure
	}

	// Reaching here means a non-Describable error escaped the typed
	// processing pipeline. After #431 this branch is structurally
	// unreachable in production; kept as a safety net for tests that
	// fabricate raw errors. Reason stays ERROR_REASON_UNSPECIFIED.
	return failure
}

// marshalOrdersForAudit returns the deterministic BUSINESS-INTENT bytes of each
// order (OrderTechnical excluded via processing.MarshalOrderBusinessIntent),
// captured once at apply time. The same byte slices feed (a) the audit hash
// chain and (b) AuditItem.SerializedOrder, so verifiers re-hash the exact bytes
// that were persisted instead of re-marshalling an Order proto — and the chain
// proves only accepted intent, never admission-derived execution metadata
// (coverage_bits, inputs_resolution_hash, preload_unavailable). The chain is
// immune to vtprotobuf or Order schema evolution. Symmetric with the idempotency
// hash (processor.hashOrder), which binds the identical projection.
func marshalOrdersForAudit(orders []*raftcmdpb.Order) [][]byte {
	out := make([][]byte, len(orders))

	for i, order := range orders {
		// nil buf makes MarshalOrderBusinessIntent allocate a fresh slice per
		// call. Each entry owns its bytes; the apply path hashes them and writes
		// them straight to Pebble. (Reusing one buffer would alias earlier
		// entries — do NOT pass a shared buffer here.)
		out[i] = processing.MarshalOrderBusinessIntent(order, nil)
	}

	return out
}

// buildAuditItems creates AuditItem entries from the pre-marshalled
// order payloads and their associated logs. For failure cases (logs is
// nil), all items get LogSequence=0.
func buildAuditItems(serializedOrders [][]byte, logs []*raftcmdpb.CreatedLogOrReference) []*auditpb.AuditItem {
	items := make([]*auditpb.AuditItem, len(serializedOrders))

	for i, payload := range serializedOrders {
		item := &auditpb.AuditItem{
			OrderIndex:      uint32(i),
			SerializedOrder: payload,
		}

		if i < len(logs) {
			if created := logs[i].GetCreatedLog(); created != nil {
				item.LogSequence = created.GetSequence()
			} else if refSeq := logs[i].GetReferenceSequence(); refSeq > 0 {
				item.LogSequence = refSeq
			}
		}

		items[i] = item
	}

	return items
}

// extractLedgers returns the distinct ledger names targeted by a set of
// orders. The wrapper-level split makes attribution structural: ledger-scoped
// orders expose their target via LedgerScopedOrder.ledger, and system-scoped
// orders never contribute a ledger name. Adding a new ledger-scoped order
// variant cannot regress audit attribution since the field lives on the
// wrapper, not on each payload.
func extractLedgers(orders []*raftcmdpb.Order) []string {
	seen := make(map[string]struct{})

	for _, order := range orders {
		ledger := order.GetLedgerScoped().GetLedger()
		if ledger != "" {
			seen[ledger] = struct{}{}
		}
	}

	if len(seen) == 0 {
		return nil
	}

	ledgers := make([]string, 0, len(seen))
	for l := range seen {
		ledgers = append(ledgers, l)
	}

	slices.Sort(ledgers)

	return ledgers
}
