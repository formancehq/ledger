package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestVerifySkippedOrder_AllowedReasonEmitsNothing exercises the happy path:
// an OrderSkippedLog whose reason is in the originating order's
// skippable_reasons whitelist passes silently. No CHECK_STORE_ERROR event
// must be produced.
func TestVerifySkippedOrder_AllowedReasonEmitsNothing(t *testing.T) {
	t.Parallel()

	expected := map[uint64][]commonpb.ErrorReason{
		7: {commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected)
	require.Empty(t, events, "an authorised skip must not emit any CHECK_STORE_ERROR")
}

// TestVerifySkippedOrder_RejectsKindInternal pins the defense-in-depth gate:
// a structural KindInternal reason is never a legitimate skip even when the
// audit-bound order somehow listed it (would already be rejected by
// admission, but the checker is the projection's safety net).
func TestVerifySkippedOrder_RejectsKindInternal(t *testing.T) {
	t.Parallel()

	expected := map[uint64][]commonpb.ErrorReason{
		7: {commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN)
	events := captureEvents(t, "L", 7, payload, expected)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsUnspecified covers the boundary case where
// the projection records UNSPECIFIED (tampered/corrupted skip log). Even if
// admission wouldn't have produced one, the checker must catch it.
func TestVerifySkippedOrder_RejectsUnspecified(t *testing.T) {
	t.Parallel()

	expected := map[uint64][]commonpb.ErrorReason{
		7: {commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	events := captureEvents(t, "L", 7, payload, expected)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsReasonOutsideWhitelist is the core tampering
// scenario: the OrderSkippedLog records a reason that the chain-bound order
// never authorised. Reproducing this is exactly what makes the LedgerLog
// projection a tampering vector — the audit chain alone wouldn't surface it.
func TestVerifySkippedOrder_RejectsReasonOutsideWhitelist(t *testing.T) {
	t.Parallel()

	expected := map[uint64][]commonpb.ErrorReason{
		7: {commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)
	events := captureEvents(t, "L", 7, payload, expected)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsMissingExpectedEntry covers fabrication:
// a skip log is recorded for a sequence whose originating order never
// authorised any skip at all. The expectedSkippable map has no entry for
// that sequence — the checker must still flag the skip as invalid.
func TestVerifySkippedOrder_RejectsMissingExpectedEntry(t *testing.T) {
	t.Parallel()

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, nil)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_IgnoresNonSkipPayloads ensures the helper is
// strictly scoped to OrderSkipped projections — any other LedgerLogPayload
// must pass through silently. (Volumes, metadata, etc. are validated by
// their own passes.)
func TestVerifySkippedOrder_IgnoresNonSkipPayloads(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}
	events := captureEvents(t, "L", 7, payload, nil)
	require.Empty(t, events)
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
	expected map[uint64][]commonpb.ErrorReason,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	events := []*servicepb.CheckStoreEvent{}

	verifySkippedOrder(ledger, seq, payload, expected, func(e *servicepb.CheckStoreEvent) {
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
