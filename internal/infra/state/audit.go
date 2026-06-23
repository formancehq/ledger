package state

import (
	"errors"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
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
		failure.ErrorType = d.Reason()

		maps.Copy(failure.GetContext(), d.Metadata())

		return failure
	}

	// Reaching here means a non-Describable error escaped the typed
	// processing pipeline. After #431 this branch is structurally
	// unreachable in production; kept as a safety net for tests that
	// fabricate raw errors.
	failure.ErrorType = "UNKNOWN"

	return failure
}

// marshalOrdersForAudit returns the deterministic bytes of each order,
// captured once at apply time. The same byte slices feed (a) the audit
// hash chain and (b) AuditItem.SerializedOrder, so verifiers re-hash
// the exact bytes that were persisted instead of re-marshalling an
// Order proto. The chain is then immune to vtprotobuf or Order schema
// evolution.
func marshalOrdersForAudit(orders []*raftcmdpb.Order) [][]byte {
	out := make([][]byte, len(orders))

	for i, order := range orders {
		// nil base makes MarshalDeterministicVT allocate a fresh slice
		// per call. Each entry owns its bytes; the apply path hashes
		// them and writes them straight to Pebble.
		out[i] = order.MarshalDeterministicVT(nil)
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

// extractLogSequenceRange returns the min and max sequence among the *created*
// logs in the slice. Idempotent ReferenceSequence entries are ignored: they
// point at logs created by older proposals, so including them would let
// AppliedProposal.{min,max}LogSequence span an unrelated range and cause the
// index builder to apply this proposal's transient exclusion set to logs that
// belong to a different proposal (NumaryBot blocker — PR #542).
//
// Returns (0, 0) when there are no CreatedLog entries (all-idempotent batch).
// Callers must skip the AppliedProposal in that case (see
// appliedProposalSync.advance — MaxLogSequence == 0 is the "no logs touched"
// sentinel).
func extractLogSequenceRange(logsOrRefs []*raftcmdpb.CreatedLogOrReference) (minSeq, maxSeq uint64) {
	for _, logOrRef := range logsOrRefs {
		created := logOrRef.GetCreatedLog()
		if created == nil {
			continue
		}

		seq := created.GetSequence()

		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return minSeq, maxSeq
}
