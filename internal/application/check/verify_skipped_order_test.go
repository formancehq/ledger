package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestVerifySkippedOrder_AllowedReasonEmitsNothing exercises the happy path:
// an OrderSkippedLog whose reason is in the originating order's
// skippable_reasons whitelist and whose reason-specific correlator replays
// successfully passes silently.
func TestVerifySkippedOrder_AllowedReasonEmitsNothing(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-x",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-x": 3},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, refs)
	require.Empty(t, events, "an authorised skip with a satisfied correlator must emit nothing")
}

// TestVerifySkippedOrder_RejectsKindInternal pins the defense-in-depth gate:
// a structural KindInternal reason is never a legitimate skip even when the
// audit-bound order somehow listed it.
func TestVerifySkippedOrder_RejectsKindInternal(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN)
	events := captureEvents(t, "L", 7, payload, expected, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsUnspecified covers the boundary case where
// the projection records UNSPECIFIED.
func TestVerifySkippedOrder_RejectsUnspecified(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	events := captureEvents(t, "L", 7, payload, expected, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsReasonOutsideWhitelist is the core tampering
// scenario: the OrderSkippedLog records a reason the chain-bound order did
// not authorise.
func TestVerifySkippedOrder_RejectsReasonOutsideWhitelist(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)
	events := captureEvents(t, "L", 7, payload, expected, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsMissingExpectedEntry covers fabrication: a
// skip log is recorded for a sequence whose originating order never
// authorised any skip at all.
func TestVerifySkippedOrder_RejectsMissingExpectedEntry(t *testing.T) {
	t.Parallel()

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, nil, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_IgnoresNonSkipPayloads ensures the helper is
// strictly scoped to OrderSkipped projections.
func TestVerifySkippedOrder_IgnoresNonSkipPayloads(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}
	events := captureEvents(t, "L", 7, payload, nil, nil)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsUnclaimedReference covers
// the central tampering scenario the checker pass was hardened against: a
// store that flipped a successful CreatedTransaction → OrderSkipped on a
// fresh reference. Without the reference replay, the whitelist check alone
// would let it pass.
func TestVerifySkippedOrder_ReferenceConflictRejectsUnclaimedReference(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-not-claimed",
		},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsLaterClaim guards against a
// tampered store that staged the reference at or after the skip's sequence —
// only earlier claims can plausibly explain a conflict at sequence S.
func TestVerifySkippedOrder_ReferenceConflictRejectsLaterClaim(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-late",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-late": 7}, // first claimed at same seq → not "before"
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, refs)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsEmptyReference catches the
// pathological case where the audited order claims a reference conflict but
// had no reference set — structurally impossible.
func TestVerifySkippedOrder_ReferenceConflictRejectsEmptyReference(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:  "L",
		},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsLedgerMismatch flags a
// tampered envelope that routes the skip to a different ledger than the
// chain-bound order targets.
func TestVerifySkippedOrder_ReferenceConflictRejectsLedgerMismatch(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L-audit",
			reference: "ref-mix",
		},
	}
	refs := map[string]map[string]uint64{
		"L-projection": {"ref-mix": 3},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L-projection", 7, payload, expected, refs)
	requireInvalidSkipEvent(t, events, 7)
}

// TestRecordReferenceClaim_FirstSequenceWins pins the
// first-claim-wins semantic: a later CreatedTransaction reusing the same
// reference (which the FSM would have already rejected) must not displace
// the original sequence, otherwise the verifier's `firstSeenSeq < seq`
// check would let some skips dodge detection.
func TestRecordReferenceClaim_FirstSequenceWins(t *testing.T) {
	t.Parallel()

	refs := make(map[string]map[string]uint64)

	recordReferenceClaim(refs, "L", 3, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Reference: "ref"},
			},
		},
	})
	recordReferenceClaim(refs, "L", 9, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Reference: "ref"},
			},
		},
	})

	require.Equal(t, uint64(3), refs["L"]["ref"])
}

func skippedPayload(reason commonpb.ErrorReason) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{Reason: reason},
		},
	}
}

func captureEvents(
	t *testing.T,
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expected map[uint64]*expectedSkippableOrder,
	refs map[string]map[string]uint64,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	events := []*servicepb.CheckStoreEvent{}

	verifySkippedOrder(ledger, seq, payload, expected, refs, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	return events
}

func requireInvalidSkipEvent(t *testing.T, events []*servicepb.CheckStoreEvent, seq uint64) {
	t.Helper()
	require.Len(t, events, 1)

	got := events[0].GetError()
	require.NotNil(t, got, "expected a CheckStoreError event")
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP, got.GetErrorType())
	require.Equal(t, seq, got.GetLogSequence())
}
