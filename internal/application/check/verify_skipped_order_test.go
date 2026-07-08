package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestVerifySkippedOrder_AllowedReasonEmitsNothing exercises the happy path:
// reason in the whitelist, prior reference claim present, AND the persisted
// context fields match the chain-bound expectations.
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

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{"ledger": "L", "reference": "ref-x"},
			},
		},
	}

	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	require.Empty(t, events, "an authorised skip with a satisfied correlator must emit nothing")
}

// TestVerifySkippedOrder_ReferenceConflictRejectsStrippedContext catches a
// tampered LedgerLog where the persisted context fields were removed.
// ErrTransactionReferenceConflict.Metadata() always writes both `ledger`
// and `reference`, so missing/empty values are a tampering signal —
// validating only "present and mismatched" lets a stripped context dodge
// the check.
func TestVerifySkippedOrder_ReferenceConflictRejectsStrippedContext(t *testing.T) {
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

	// Context entirely missing.
	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)
	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)

	// Only one field stripped.
	payload = &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{"ledger": "L"}, // reference missing
			},
		},
	}

	events = captureEvents(t, "L", 7, payload, expected, refs, false)
	requireInvalidSkipEvent(t, events, 7)
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

// TestVerifySkippedOrder_RejectsWhitelistedReasonWithoutReplayBranch pins
// the fail-closed default: if admission adds a new reason to
// allowedSkippableReasons without extending verifySkippedOrder's
// reason-specific switch, the checker must flag the projection rather
// than silently accepting it after the whitelist-membership check.
//
// TRANSACTION_ALREADY_REVERTED is KindConflict (not KindInternal), so it
// passes the earlier defense-in-depth gates and reaches the switch.
// Today verifySkippedOrder has no case for it — the `default` branch
// fires INVALID_SKIP.
func TestVerifySkippedOrder_RejectsWhitelistedReasonWithoutReplayBranch(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED

	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{reason}, ledger: "L"},
	}

	payload := skippedPayload(reason)
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

// TestVerifyExpectedSkipNotElided_RejectsTamperedCreatedTransaction is the
// inverse-direction sibling of the OrderSkipped tampering tests above: a
// chain-bound order opted into TRANSACTION_REFERENCE_CONFLICT skip AND the
// audit-derived references show the reference was claimed earlier, so the
// FSM MUST have skipped — yet the persisted LedgerLog at this sequence is a
// CreatedTransaction. The LedgerLog is not hash-chain bound, so without this
// check a tamperer could elide the skip and forge a successful landing.
func TestVerifyExpectedSkipNotElided_RejectsTamperedCreatedTransaction(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-claimed-earlier",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-claimed-earlier": 3},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}

	events := captureDispatchEvents(t, "L", 7, payload, expected, refs)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifyExpectedSkipNotElided_AcceptsLegitimateCreatedTransaction pins the
// happy path: when the order opted into skip but the reference was not
// claimed before, the FSM correctly emitted a CreatedTransaction and the
// verifier stays silent.
func TestVerifyExpectedSkipNotElided_AcceptsLegitimateCreatedTransaction(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-first-claim",
		},
	}
	// ref-first-claim is claimed at seq=7 itself — no earlier claim.
	refs := map[string]map[string]uint64{
		"L": {"ref-first-claim": 7},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}

	events := captureDispatchEvents(t, "L", 7, payload, expected, refs)
	require.Empty(t, events, "first-claim CreatedTransaction must round-trip silently")
}

// TestVerifyExpectedSkipNotElided_NoExpectedEntryStaysSilent pins that a
// non-skip projection is fine when the originating order never opted into
// skip — the FSM had no option to skip, the CreatedTransaction is correct.
func TestVerifyExpectedSkipNotElided_NoExpectedEntryStaysSilent(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}

	events := captureDispatchEvents(t, "L", 7, payload, nil, nil)
	require.Empty(t, events)
}

// TestVerifyExpectedSkipNotElided_ArchiveDoesNotSuppressLiveProof pins the
// fix for a false-negative in the inverse direction: when archived chapters
// exist BUT the live audit range already proves the reference was claimed
// before the skip's sequence (firstSeenSeq < seq), the elision is a
// hash-chain-proven tamper — the archive-boundary permissiveness must NOT
// downgrade it to a silent pass. The `!claimed || firstSeenSeq >= seq`
// guard above already covers the genuinely archive-only case.
func TestVerifyExpectedSkipNotElided_ArchiveDoesNotSuppressLiveProof(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-claimed-earlier",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-claimed-earlier": 3},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}

	// Dispatched via the outer elision check — the archive flag is not an
	// input to verifyExpectedSkipNotElided anymore (see #1 fix).
	events := captureDispatchEvents(t, "L", 7, payload, expected, refs)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifyExpectedSkipNotElided_PermissiveWhenReferenceUnknown pins the
// legitimate archive-boundary permissiveness path: when the reference is
// NOT in chainBoundReferences (no live proof of a prior claim), the
// inverse check stays quiet — the prior claim may live in a purged
// chapter we cannot re-verify, and the forward direction still catches a
// forged skip.
func TestVerifyExpectedSkipNotElided_PermissiveWhenReferenceUnknown(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-archived-only",
		},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{},
		},
	}

	events := captureDispatchEvents(t, "L", 7, payload, expected, nil)
	require.Empty(t, events, "the inverse check must stay permissive when the reference is not visible in the live chain")
}

// TestVerifySkippedOrder_RejectsNilInnerOrderSkipped catches the malformed
// projection where the OrderSkipped oneof discriminant is set but the inner
// OrderSkippedLog message is nil. dispatchElisionCheck classifies "oneof
// set" as a valid skip projection and defers to the forward verifier, so a
// silent return here would let a tampered log evade every check.
func TestVerifySkippedOrder_RejectsNilInnerOrderSkipped(t *testing.T) {
	t.Parallel()

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: nil,
		},
	}

	// Fires regardless of whether the seq is expected — a nil inner message
	// is always an invalid projection, not just when a skip was authorised.
	events := captureEvents(t, "L", 7, payload, nil, nil, false)
	requireInvalidSkipEvent(t, events, 7)

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-x",
		},
	}

	events = captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestDispatchElisionCheck_FiresOnMalformedPayloadShapes pins the bypass
// closure: the elision check must fire at every seq where a skip was
// expected regardless of the actual log's shape. Previously the inline
// call inside the Apply-branch payload switch made non-Apply, nil
// Apply.Log, and nil Data payloads escape the check entirely — a tamperer
// could then rewrite the LedgerLog to any of those shapes without
// tripping Check().
func TestDispatchElisionCheck_FiresOnMalformedPayloadShapes(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-claimed-earlier",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-claimed-earlier": 3},
	}

	dispatchWithLog := func(log *commonpb.Log) []*servicepb.CheckStoreEvent {
		events := []*servicepb.CheckStoreEvent{}
		dispatchElisionCheck(7, log, expected, refs, func(e *servicepb.CheckStoreEvent) {
			events = append(events, e)
		})

		return events
	}

	tampers := map[string]*commonpb.Log{
		"nil payload":       {Sequence: 7},
		"non-Apply payload": {Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "L"}}}},
		"nil Apply":         {Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{}}},
		"nil Apply.Log":     {Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "L"}}}},
		"nil Log.Data":      {Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: "L", Log: &commonpb.LedgerLog{}}}}},
	}

	for name, log := range tampers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			events := dispatchWithLog(log)
			requireInvalidSkipEvent(t, events, 7)
		})
	}
}

// TestDispatchElisionCheck_SilentOnValidSkip pins that a well-formed
// OrderSkipped projection is accepted by the outer dispatch — the forward
// pass (verifySkippedOrder called from the Apply branch of the iteration)
// owns the validation for skip payloads.
func TestDispatchElisionCheck_SilentOnValidSkip(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-claimed-earlier",
		},
	}
	refs := map[string]map[string]uint64{
		"L": {"ref-claimed-earlier": 3},
	}

	log := &commonpb.Log{
		Sequence: 7,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "L",
					Log: &commonpb.LedgerLog{
						Data: &commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_OrderSkipped{
								OrderSkipped: &commonpb.OrderSkippedLog{Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
							},
						},
					},
				},
			},
		},
	}

	events := []*servicepb.CheckStoreEvent{}
	dispatchElisionCheck(7, log, expected, refs, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	require.Empty(t, events, "outer dispatch must defer to the forward-direction verifier for well-formed OrderSkipped projections")
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
// archive boundary escape hatch: when archived chapters exist AND the
// baseline references could not be loaded, a missing claim cannot be
// distinguished from a purged one — the verifier stays permissive.
// Conversely, when a baseline is available (callers pass
// archivedWithoutBaseline=false), the fold has already injected
// archived references into chainBoundReferences, so a missing reference
// IS fabrication and must fail loud.
//
// The payload provides matching context fields so the check exercises
// the reference-claim lookup path, not the (independently verified)
// context-field validation.
func TestVerifySkippedOrder_ReferenceConflictPermissiveWhenArchived(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-archived",
		},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":    "L",
					"reference": "ref-archived",
				},
			},
		},
	}

	// archivedWithoutBaseline=false (no archive OR baseline available) →
	// strict: missing claim is fabrication.
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)

	// archivedWithoutBaseline=true (archive AND no baseline) → permissive:
	// the claim may live in a purged chapter we cannot verify.
	events = captureEvents(t, "L", 7, payload, expected, nil, true)
	require.Empty(t, events, "missing-claim skips must pass only when archived chapters AND no baseline")
}

// TestVerifySkippedOrder_ContextTamperingCaughtEvenUnderArchiveEscape pins
// the fix for a tampering vector where the archive-escape (missing claim
// + archived chapters) would suppress the context-field checks. The
// expected `ledger` and `reference` are chain-bound and re-derivable
// regardless of whether the prior claim is visible in the live chain,
// so their validation must run BEFORE the archive escape.
func TestVerifySkippedOrder_ContextTamperingCaughtEvenUnderArchiveEscape(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-archived",
		},
	}

	// Context "reference" is tampered — should be "ref-archived" per the
	// chain-bound order. archivedWithoutBaseline=true would previously
	// early-return before the context check.
	tamperedReference := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":    "L",
					"reference": "ref-forged",
				},
			},
		},
	}

	events := captureEvents(t, "L", 7, tamperedReference, expected, nil, true)
	requireInvalidSkipEvent(t, events, 7)

	// Same for a tampered ledger field.
	tamperedLedger := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":    "L-forged",
					"reference": "ref-archived",
				},
			},
		},
	}

	// Note: expected.ledger != log's ledger triggers the top-level ledger
	// cross-check first (line ~2002). To isolate the context-field path,
	// keep expected.ledger==log ledger and only forge the context slot.
	events = captureEvents(t, "L", 7, tamperedLedger, expected, nil, true)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictBaselineSeededReference pins the
// fold semantic: when foldBaselineReferences seeds an archived reference
// with sentinel sequence 0, verifySkippedOrder accepts the live skip
// against it — sentinel 0 always precedes the skip's live seq, so the
// firstSeenSeq < seq guard passes.
func TestVerifySkippedOrder_ReferenceConflictBaselineSeededReference(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref-from-baseline",
		},
	}
	// Sentinel 0 represents a reference folded from baseline (archive).
	refs := map[string]map[string]uint64{
		"L": {"ref-from-baseline": 0},
	}

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":    "L",
					"reference": "ref-from-baseline",
				},
			},
		},
	}

	// archivedWithoutBaseline=false (baseline was available and folded):
	// the sentinel-seeded reference is enough to satisfy the check.
	events := captureEvents(t, "L", 7, payload, expected, refs, false)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_HandlesNilExpectedMaps guards against the panic
// path NumaryBot flagged on b6e8fd064: a corrupted store with a readable
// baseline triggers a hash mismatch in verifyAuditHashChain, which used
// to return nil maps; foldBaselineReferences then assigned into a nil
// chainBoundReferences and crashed. The fix returns the partially
// populated maps from verifyAuditHashChain — this test pins that
// verifySkippedOrder itself stays panic-safe when the expected* maps
// are empty/nil, since Check() keeps running after a chain break and
// may still encounter skip logs.
func TestVerifySkippedOrder_HandlesNilExpectedMaps(t *testing.T) {
	t.Parallel()

	payload := skippedPayload(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)

	require.NotPanics(t, func() {
		captureEvents(t, "L", 7, payload, nil, nil, false)
	})
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

// TestCollectExpectedSkippable_TracksMirrorIngestedReferences pins that the
// verifier accepts a skip whose conflicting reference was claimed by a
// mirror ingestion rather than a regular CreateTransaction. Mirror
// promotion replays the source ledger's reference writes through
// MirrorIngestOrder.Entry.CreatedTransaction; processMirrorCreatedTransaction
// calls the same PutTransactionReference as the regular path, so the
// verifier must derive the claim from both order shapes — otherwise
// Check() false-positives INVALID_SKIP on legitimate mirror stores.
func TestCollectExpectedSkippable_TracksMirrorIngestedReferences(t *testing.T) {
	t.Parallel()

	mirrorIngestOrder := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
								CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
									TransactionId: 7,
									Reference:     "mirror-ref",
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := mirrorIngestOrder.MarshalVT()
	require.NoError(t, err)

	items := []*auditpb.AuditItem{
		{SerializedOrder: body, LogSequence: 50},
	}

	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	refs := make(map[string]map[string]uint64)

	collectExpectedSkippable(items, expectedSkip, refs)

	require.Equal(t, uint64(50), refs["L"]["mirror-ref"],
		"mirror-ingested reference must be recorded at its log sequence so later skip verifiers see the prior claim")
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

// captureDispatchEvents wraps dispatchElisionCheck with a canonical Apply-
// shaped Log so tests can exercise the outer-scope elision guard against a
// LedgerLogPayload (the same input verifySkippedOrder receives). Tests that
// need to model non-Apply / malformed log shapes construct the *commonpb.Log
// directly and call dispatchElisionCheck.
func captureDispatchEvents(
	t *testing.T,
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expected map[uint64]*expectedSkippableOrder,
	refs map[string]map[string]uint64,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	log := &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Data: payload,
					},
				},
			},
		},
	}

	events := []*servicepb.CheckStoreEvent{}

	dispatchElisionCheck(seq, log, expected, refs, func(e *servicepb.CheckStoreEvent) {
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
