package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestVerifySkippedOrder_AllowedReasonEmitsNothing exercises the happy path.
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
	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	require.Empty(t, events, "an authorised skip with a satisfied correlator must emit nothing")
}

// TestVerifySkippedOrder_RejectsKindInternal pins the defense-in-depth gate.
func TestVerifySkippedOrder_RejectsKindInternal(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INVALID_EXECUTION_PLAN)
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsUnspecified covers UNSPECIFIED tampering.
func TestVerifySkippedOrder_RejectsUnspecified(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED)
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsReasonOutsideWhitelist.
func TestVerifySkippedOrder_RejectsReasonOutsideWhitelist(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS)
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RejectsMissingExpectedEntry covers fabrication.
func TestVerifySkippedOrder_RejectsMissingExpectedEntry(t *testing.T) {
	t.Parallel()

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, nil, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_IgnoresNonSkipPayloads.
func TestVerifySkippedOrder_IgnoresNonSkipPayloads(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}
	events := captureEvents(t, "L", 7, payload, nil, nil, false)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsUnclaimedReference covers
// the central tampering scenario the checker pass was hardened against: a
// store that flipped a successful CreatedTransaction → OrderSkipped on a
// fresh reference. Without the audit-derived replay, the whitelist check
// alone would let it pass.
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
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsLaterClaim guards against
// references staged at or after the skip's sequence — only earlier claims
// can plausibly explain a conflict at sequence S.
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
	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsEmptyReference catches the
// pathological case where the audited order claims a reference conflict
// but had no reference set.
func TestVerifySkippedOrder_ReferenceConflictRejectsEmptyReference(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:  "L",
		},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
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
	events := captureEvents(t, "L-projection", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsTamperedContextReference
// catches a tampered LedgerLog projection where the context.reference was
// flipped to point at an unrelated R'. The LedgerLog is not hash-bound, so
// only the audit chain's order.reference can be trusted as the ground
// truth.
func TestVerifySkippedOrder_ReferenceConflictRejectsTamperedContextReference(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-chain",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-chain": 3},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{"reference": "ref-tampered"},
			},
		},
	}

	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictRejectsTamperedContextLedger
// catches a tampered context.ledger field.
func TestVerifySkippedOrder_ReferenceConflictRejectsTamperedContextLedger(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref": 3},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{"ledger": "L-tampered"},
			},
		},
	}

	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictAcceptsMatchingContext pins the
// happy-path round trip for context: when the persisted context.reference
// and context.ledger match the chain-bound values, the verifier passes.
func TestVerifySkippedOrder_ReferenceConflictAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref": 3},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":                "L",
					"reference":             "ref",
					"existingTransactionId": "42", // not verifiable from chain alone — must not fail the check
				},
			},
		},
	}

	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	require.Empty(t, events, "matching context must round-trip even with non-verifiable fields like existingTransactionId")
}

// TestVerifySkippedOrder_ReferenceConflictPermissiveWhenArchived pins the
// archive boundary escape hatch: with archived chapters present, a missing
// claim cannot be distinguished from one that lived in a purged chapter,
// so the verifier downgrades the missing-reference branch to a silent
// pass to avoid false positives on legitimate skips against archived
// references.
func TestVerifySkippedOrder_ReferenceConflictPermissiveWhenArchived(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-archived",
		},
	}

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)

	// hasArchivedChapters=false → fails loud (no archive escape)
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)

	// hasArchivedChapters=true → permissive (can't tell if claim was purged)
	events = captureEvents(t, "L", 7, payload, expected, nil, true)
	require.Empty(t, events, "missing-claim skips must pass when archived chapters exist")
}

// TestCollectExpectedSkippable_RecordsReferencesFromChain pins the
// audit-derived reference tracking: every chain-bound CreateTransactionOrder
// reference is recorded regardless of whether the order opted into skip,
// and the FIRST audit log sequence wins (re-claims later on the same
// reference do not move the claim — otherwise the verifier's
// `firstSeenSeq < seq` check would let some skips dodge detection).
func TestCollectExpectedSkippable_RecordsReferencesFromChain(t *testing.T) {
	t.Parallel()

	order := func(ref string, skipReasons ...commonpb.ErrorReason) *raftcmdpb.Order {
		return &raftcmdpb.Order{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "L",
					Payload: &raftcmdpb.LedgerScopedOrder_Apply{
						Apply: &raftcmdpb.LedgerApplyOrder{
							Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{Reference: ref},
							},
						},
					},
				},
			},
			SkippableReasons: skipReasons,
		}
	}

	item := func(o *raftcmdpb.Order, logSeq uint64) *auditpb.AuditItem {
		b, err := o.MarshalVT()
		require.NoError(t, err)

		return &auditpb.AuditItem{SerializedOrder: b, LogSequence: logSeq}
	}

	items := []*auditpb.AuditItem{
		item(order("ref-A"), 100), // strict CreateTransaction at log 100
		item(order("ref-B", commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT), 101), // skip-tolerant at log 101
		item(order("ref-A"), 102), // duplicate ref-A at log 102 → must NOT shift the first claim
		item(order("", commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT), 103),    // empty reference → not tracked
		item(order("ref-C", commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT), 0), // failure-side (LogSequence=0) → ignored
	}

	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	refs := make(map[string]map[string]uint64)

	collectExpectedSkippable(items, expectedSkip, refs)

	require.Equal(t, uint64(100), refs["L"]["ref-A"], "first claim must win for re-claimed reference")
	require.Equal(t, uint64(101), refs["L"]["ref-B"])
	_, hasC := refs["L"]["ref-C"]
	require.False(t, hasC, "items with LogSequence=0 (failure-side) must not contribute references")

	// Only items with skippable_reasons AND a non-zero LogSequence are in expectedSkip.
	require.Len(t, expectedSkip, 2)
	require.Equal(t, "ref-B", expectedSkip[101].reference)
	require.Equal(t, "L", expectedSkip[101].ledger)
}

// TestCollectExpectedSkippable_HonoursItemLogSequence pins the fix to the
// MinLogSequence+i indexing bug: when audit items contain interleaved
// idempotency-replay ReferenceSequence entries, the verifier must read
// each item's own LogSequence (set by buildAuditItems) rather than
// extrapolating linearly from MinLogSequence.
func TestCollectExpectedSkippable_HonoursItemLogSequence(t *testing.T) {
	t.Parallel()

	body := func() []byte {
		b, err := (&raftcmdpb.Order{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "L",
					Payload: &raftcmdpb.LedgerScopedOrder_Apply{
						Apply: &raftcmdpb.LedgerApplyOrder{
							Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{Reference: "r"},
							},
						},
					},
				},
			},
			SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		}).MarshalVT()
		require.NoError(t, err)

		return b
	}()

	// Item 0 is an idempotency replay pointing back to log 80, item 1 is a
	// fresh CreatedLog at 200. The MinLogSequence+i formula would record
	// item 1's whitelist under log 201, missing the actual skip log at 200.
	items := []*auditpb.AuditItem{
		{SerializedOrder: body, LogSequence: 80},  // ReferenceSequence replay
		{SerializedOrder: body, LogSequence: 200}, // CreatedLog
	}

	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	refs := make(map[string]map[string]uint64)

	collectExpectedSkippable(items, expectedSkip, refs)

	require.Contains(t, expectedSkip, uint64(80))
	require.Contains(t, expectedSkip, uint64(200))
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
	hasArchivedChapters bool,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	events := []*servicepb.CheckStoreEvent{}

	verifySkippedOrder(ledger, seq, payload, expected, refs, hasArchivedChapters, func(e *servicepb.CheckStoreEvent) {
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
