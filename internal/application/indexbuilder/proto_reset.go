package indexbuilder

import (
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// resetLogForReuse prepares a *commonpb.Log for reuse by UnmarshalVT without
// calling proto.Reset. This preserves nested message allocations across
// iterations, avoiding ~15-20 heap allocations per log entry in the hot loop.
//
// The function zeroes all scalar and optional fields that might not be
// overwritten by UnmarshalVT (proto3 omits default values from the wire),
// truncates repeated fields to len=0 (keeping backing array capacity), and
// clears maps. Singular message pointers on the "always present" path are
// preserved so UnmarshalVT reuses them instead of allocating.
//
// Safety: the function must reset every field that could carry stale data.
// Fields always present in the wire format are safe to skip (UnmarshalVT
// overwrites them), but optional fields MUST be nil'd/zeroed.
func resetLogForReuse(msg proto.Message) {
	m, ok := msg.(*commonpb.Log)
	if !ok {
		proto.Reset(msg)

		return
	}

	// Log-level fields.
	m.Sequence = 0
	m.Hash = m.Hash[:0]
	m.Receipt = ""
	m.Idempotency = nil
	m.Signature = nil
	m.ResponseSignature = nil

	// Preserve m.Payload (always present in system logs).
	if m.Payload == nil {
		return
	}

	resetLogPayload(m.Payload)
}

func resetLogPayload(p *commonpb.LogPayload) {
	apply, ok := p.Type.(*commonpb.LogPayload_Apply)
	if !ok {
		// Non-Apply log type: nil the oneof to force reallocation.
		// These are rare (CreateLedger, DeleteLedger, etc.).
		p.Type = nil

		return
	}

	// Preserve the Apply wrapper and its inner ApplyLedgerLog.
	if apply.Apply == nil {
		return
	}

	a := apply.Apply
	a.LedgerName = ""

	// Preserve a.Log (always present).
	if a.Log == nil {
		return
	}

	resetLedgerLog(a.Log)
}

func resetLedgerLog(ll *commonpb.LedgerLog) {
	ll.Id = 0
	// Preserve ll.Date (always present in apply logs).
	// Preserve ll.Data (always present).

	if ll.Data == nil {
		return
	}

	resetLedgerLogPayload(ll.Data)
}

func resetLedgerLogPayload(p *commonpb.LedgerLogPayload) {
	// Handle each oneof variant. Preserve the wrapper and inner message
	// for the common types (CreatedTransaction, RevertedTransaction) so
	// UnmarshalVT reuses them when the next log is the same type.
	switch v := p.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		resetCreatedTransaction(v.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		resetRevertedTransaction(v.RevertedTransaction)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		resetSavedMetadata(v.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		resetDeletedMetadata(v.DeletedMetadata)
	default:
		// Rare types (SetMetadataFieldType, CreateIndex, etc.): nil the
		// oneof. These don't benefit from reuse and are infrequent.
		p.Payload = nil
	}
}

func resetCreatedTransaction(ct *commonpb.CreatedTransaction) {
	if ct == nil {
		return
	}

	ct.PeriodId = 0
	ct.PostCommitVolumes = nil
	ct.Warnings = ct.Warnings[:0]
	clear(ct.AccountMetadata)
	clear(ct.PreviousAccountMetadata)

	resetTransaction(ct.Transaction)
}

func resetRevertedTransaction(rt *commonpb.RevertedTransaction) {
	if rt == nil {
		return
	}

	rt.RevertedTransactionId = 0
	rt.PostCommitVolumes = nil
	rt.Warnings = rt.Warnings[:0]

	resetTransaction(rt.RevertTransaction)
}

func resetTransaction(txn *commonpb.Transaction) {
	if txn == nil {
		return
	}

	txn.Id = 0
	txn.Reference = ""
	txn.Reverted = false
	txn.Postings = txn.Postings[:0]
	// Nil optional message fields that might be absent in the next log.
	txn.Metadata = nil
	txn.UpdatedAt = nil
	txn.RevertedAt = nil
	// Preserve txn.Timestamp and txn.InsertedAt (always present).
}

func resetSavedMetadata(sm *commonpb.SavedMetadata) {
	if sm == nil {
		return
	}

	sm.Target = nil
	sm.Warnings = sm.Warnings[:0]
	clear(sm.PreviousValues)

	if sm.Metadata != nil {
		sm.Metadata.Metadata = sm.Metadata.Metadata[:0]
	}
}

func resetDeletedMetadata(dm *commonpb.DeletedMetadata) {
	if dm == nil {
		return
	}

	dm.Target = nil
	dm.Key = ""
	dm.PreviousValue = nil
}
