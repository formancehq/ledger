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

// buildAuditItems creates AuditItem entries from orders and their associated logs.
// For failure cases (logs is nil), all items get LogSequence=0.
func buildAuditItems(orders []*raftcmdpb.Order, logs []*raftcmdpb.CreatedLogOrReference) []*auditpb.AuditItem {
	items := make([]*auditpb.AuditItem, len(orders))

	for i, order := range orders {
		item := &auditpb.AuditItem{
			OrderIndex: uint32(i),
			Order:      order,
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

// extractLedgers returns the distinct ledger names targeted by a set of orders.
func extractLedgers(orders []*raftcmdpb.Order) []string {
	seen := make(map[string]struct{})

	for _, order := range orders {
		var ledger string

		switch {
		case order.GetApply() != nil:
			ledger = order.GetApply().GetLedger()
		case order.GetCreateLedger() != nil:
			ledger = order.GetCreateLedger().GetName()
		case order.GetDeleteLedger() != nil:
			ledger = order.GetDeleteLedger().GetName()
		case order.GetMirrorIngest() != nil:
			ledger = order.GetMirrorIngest().GetLedger()
		case order.GetPromoteLedger() != nil:
			ledger = order.GetPromoteLedger().GetLedger()
		case order.GetSaveLedgerMetadata() != nil:
			ledger = order.GetSaveLedgerMetadata().GetLedger()
		case order.GetDeleteLedgerMetadata() != nil:
			ledger = order.GetDeleteLedgerMetadata().GetLedger()
		}

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

// extractLogSequenceRange returns the min and max log sequence from a slice of
// CreatedLogOrReference. For created logs it uses the log sequence; for
// idempotent references it uses the reference sequence. Returns (0, 0) if empty.
func extractLogSequenceRange(logsOrRefs []*raftcmdpb.CreatedLogOrReference) (minSeq, maxSeq uint64) {
	for _, logOrRef := range logsOrRefs {
		var seq uint64
		if created := logOrRef.GetCreatedLog(); created != nil {
			seq = created.GetSequence()
		} else {
			seq = logOrRef.GetReferenceSequence()
		}

		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}

		if seq > maxSeq {
			maxSeq = seq
		}
	}

	return minSeq, maxSeq
}
