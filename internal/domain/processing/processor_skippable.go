package processing

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// matchOrderSkip checks whether the error returned by a sub-processor can be
// converted into an OrderSkippedLog given the order's `skippable_reasons`
// whitelist. When it matches, it returns the LogPayload to emit in place of
// the failure (wrapped appropriately for the order type) and true. Otherwise
// it returns (nil, false) and ProcessOrders propagates the original error.
//
// Structural reasons (domain.KindInternal) are never honoured here even if
// admission failed to strip them from the list — defense in depth.
//
// Mutation safety: the rollback of mutations made by a skipped order is the
// responsibility of orderOverlayScope. ProcessOrders allocates one per
// skip-tolerant order and discards it (no Commit) on skip. Sub-processors do
// not need to perform their reads "dry" anymore; the overlay buffers their
// reads-after-writes and drops the buffer on rollback.
func matchOrderSkip(order *raftcmdpb.Order, err domain.Describable) (*commonpb.LogPayload, bool) {
	allowed := order.GetSkippableReasons()
	if len(allowed) == 0 {
		return nil, false
	}

	target := domain.ReasonCode(err.Reason())
	if target == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return nil, false
	}

	if domain.KindForReason(target) == domain.KindInternal {
		return nil, false
	}

	matched := slices.Contains(allowed, target)

	if !matched {
		return nil, false
	}

	// Surface the matched error's structured metadata as the
	// OrderSkippedLog.context so clients can correlate (existing tx id,
	// reference, …) without an out-of-band lookup. Sub-processors are free
	// to expose additional fields by enriching their Metadata().
	var ctx map[string]string
	if md := err.Metadata(); len(md) > 0 {
		ctx = maps.Clone(md)
	}

	skipPayload := &commonpb.OrderSkippedLog{
		Reason:  target,
		Context: ctx,
	}

	return wrapSkippedPayloadForOrder(order, skipPayload), true
}

// wrapSkippedPayloadForOrder builds the LogPayload envelope that
// ProcessOrders returns for a skipped order, mirroring the wrapping each
// sub-dispatch (LedgerScoped Apply, etc.) produces on the success path so
// downstream consumers see a uniform Log shape. Id and Date stay zero
// here — assignSkipLogIDAndDate fills them once ProcessOrders has the
// parent Scope in hand (the inner LedgerLog must carry a per-ledger id
// because the read-side index keys per-ledger logs by it).
func wrapSkippedPayloadForOrder(order *raftcmdpb.Order, skipped *commonpb.OrderSkippedLog) *commonpb.LogPayload {
	ledgerName := ""

	if lso, ok := order.GetType().(*raftcmdpb.Order_LedgerScoped); ok && lso.LedgerScoped != nil {
		ledgerName = lso.LedgerScoped.GetLedger()
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledgerName,
				Log: &commonpb.LedgerLog{
					Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_OrderSkipped{
							OrderSkipped: skipped,
						},
					},
				},
			},
		},
	}
}

// assignSkipLogIDAndDate allocates the ledger-local Log id and date for a
// skipped order's log on the PARENT scope (never on the overlay — the
// overlay is dropped to roll back the failed sub-handler's writes, so any
// boundary update inside it would be lost).
//
// This mirrors the post-success epilogue in processApply: bump
// boundaries.NextLogId, write the boundaries back, record the date. Without
// it every skipped log lands at LedgerLog.Id=0 and the read-side index
// (which keys per-ledger logs by (ledger, log_id) — see
// internal/application/indexbuilder/process_logs.go) silently overwrites
// every prior skip on the same ledger.
//
// Only LedgerScoped Apply orders are currently allowed to opt into
// skippable_reasons (admission's per-operation whitelist), so the helper
// expects a LedgerScoped order with a non-empty ledger name. Anything else
// is a structural invariant violation: surface it loudly instead of
// silently shipping a log with Id=0.
func assignSkipLogIDAndDate(parent Scope, order *raftcmdpb.Order, payload *commonpb.LogPayload) domain.Describable {
	lso, ok := order.GetType().(*raftcmdpb.Order_LedgerScoped)
	if !ok || lso.LedgerScoped == nil || lso.LedgerScoped.GetLedger() == "" {
		return &domain.ErrInvalidExecutionPlan{Reason_: fmt.Sprintf("skip allocated for non-LedgerScoped order %T", order.GetType())}
	}

	ledger := lso.LedgerScoped.GetLedger()

	apply := payload.GetApply()
	if apply == nil || apply.GetLog() == nil {
		return &domain.ErrInvalidExecutionPlan{Reason_: fmt.Sprintf("skip payload for ledger %q has no inner Apply/Log envelope", ledger)}
	}

	boundariesReader, err := parent.GetBoundaries(ledger)
	if err != nil {
		// ErrNotFound means the apply order references a ledger that does
		// not exist — that should have surfaced as the sub-handler's
		// failure, never as a skip. Either way, refusing to forge a log
		// for a non-existent ledger is safer than silently emitting one
		// with arbitrary boundaries.
		if errors.Is(err, domain.ErrNotFound) {
			return &domain.ErrInvalidExecutionPlan{Reason_: fmt.Sprintf("skip allocated for unknown ledger %q", ledger)}
		}

		return &domain.ErrStorageOperation{Operation: fmt.Sprintf("loading boundaries for skip log on ledger %q", ledger), Cause: err}
	}

	boundaries := boundariesReader.Mutate()
	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1

	parent.PutBoundaries(ledger, boundaries)

	apply.Log.Id = nextLogID
	apply.Log.Date = parent.GetDate().Mutate()

	return nil
}
