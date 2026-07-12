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
// LEDGER_ALREADY_EXISTS is KindAlreadyExists (not KindInternal), so it
// passes the earlier defense-in-depth gates and reaches the switch. No
// LedgerAction annotates it as skippable today — the .proto plugin
// would never emit it into a real Order.SkippableReasons — but the
// checker MUST still reject a tampered projection that presents it
// under a chain-bound Order that somehow lists it. If a future PR
// legitimately whitelists LEDGER_ALREADY_EXISTS, swap the reason for
// another non-KindInternal reason that still has no case.
func TestVerifySkippedOrder_RejectsWhitelistedReasonWithoutReplayBranch(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_LEDGER_ALREADY_EXISTS

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

// TestVerifyExpectedSkipNotElided_MetadataBaselineCoveredAbsenceIsDefinitive
// pins the #12 fix: for METADATA_NOT_FOUND, an empty LIVE metadata timeline
// under archived chapters is ambiguous ONLY when the baseline fold could not
// cover the archived range. When the baseline IS available it already covers
// the archive, so an empty live timeline is definitive proof of absence — a
// non-skip projection at that seq is a proven elision and must be flagged.
func TestVerifyExpectedSkipNotElided_MetadataBaselineCoveredAbsenceIsDefinitive(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}

	// Empty live metadata timeline: the key was never Set/Deleted in the
	// live audit range (mutationStateWithWitness → present=false,
	// witnessed=false).
	chainBound := newChainBoundState()

	const hasArchivedChapters = true

	// Baseline UNAVAILABLE → ambiguous, stay permissive (an archived Set we
	// cannot see could have made the delete succeed).
	var permissive []*servicepb.CheckStoreEvent
	verifyExpectedSkipNotElided("L", 7, expected, chainBound, hasArchivedChapters, false, func(e *servicepb.CheckStoreEvent) {
		permissive = append(permissive, e)
	})
	require.Empty(t, permissive, "baseline unavailable: empty-live-under-archives must stay permissive")

	// Baseline AVAILABLE → the baseline covers the archive; empty-live is
	// definitive absence, so the elision must be flagged.
	var strict []*servicepb.CheckStoreEvent
	verifyExpectedSkipNotElided("L", 7, expected, chainBound, hasArchivedChapters, true, func(e *servicepb.CheckStoreEvent) {
		strict = append(strict, e)
	})
	requireInvalidSkipEvent(t, strict, 7)
}

// TestVerifyExpectedSkipNotElided_AccountTypeBaselineCoveredAbsenceIsDefinitive
// is the account-type sibling of the metadata test above (ACCOUNT_TYPE_NOT_FOUND):
// empty live account-type timeline under archives is ambiguous without a
// baseline but definitive once the baseline covers the archived range.
func TestVerifyExpectedSkipNotElided_AccountTypeBaselineCoveredAbsenceIsDefinitive(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	// Empty live account-type timeline: ACCOUNT_TYPE_NOT_FOUND expects the
	// type absent (mustBePresent=false), which the empty timeline satisfies.
	chainBound := newChainBoundState()

	const hasArchivedChapters = true

	var permissive []*servicepb.CheckStoreEvent
	verifyExpectedSkipNotElided("L", 7, expected, chainBound, hasArchivedChapters, false, func(e *servicepb.CheckStoreEvent) {
		permissive = append(permissive, e)
	})
	require.Empty(t, permissive, "baseline unavailable: empty-live-under-archives must stay permissive")

	var strict []*servicepb.CheckStoreEvent
	verifyExpectedSkipNotElided("L", 7, expected, chainBound, hasArchivedChapters, true, func(e *servicepb.CheckStoreEvent) {
		strict = append(strict, e)
	})
	requireInvalidSkipEvent(t, strict, 7)
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
		dispatchElisionCheck(7, log, expected, chainBoundStateFromRefs(refs), false, false, func(e *servicepb.CheckStoreEvent) {
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
	dispatchElisionCheck(7, log, expected, chainBoundStateFromRefs(refs), false, false, func(e *servicepb.CheckStoreEvent) {
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
// happy-path round trip for context: when the persisted context.reference,
// context.ledger AND context.existingTransactionId match the chain-bound
// values, the verifier passes.
func TestVerifySkippedOrder_ReferenceConflictAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref",
		},
	}
	// The owning tx id (42) is now re-derivable from audit-bound data —
	// the checker records it when it replays the first successful claim.
	cb := chainBoundStateFromRefsAndTxIDs(
		map[string]map[string]uint64{"L": {"ref": 3}},
		map[string]map[string]uint64{"L": {"ref": 42}},
	)

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":                "L",
					"reference":             "ref",
					"existingTransactionId": "42",
				},
			},
		},
	}

	events := captureEventsState(t, "L", 7, payload, expected, cb, false)
	require.Empty(t, events, "matching context (including existingTransactionId) must round-trip")
}

// TestVerifySkippedOrder_ReferenceConflictRejectsTamperedExistingTxID pins
// invariant #8 for the client-facing existingTransactionId projection field:
// a stored skip log whose context misattributes the conflicting transaction
// (context.existingTransactionId != the audit-derived owner) is flagged
// INVALID_SKIP. Without this, a tamperer could rewrite the field in the
// non-hash-bound LedgerLog to mislead a client about which transaction owns
// the reference.
func TestVerifySkippedOrder_ReferenceConflictRejectsTamperedExistingTxID(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref",
		},
	}
	cb := chainBoundStateFromRefsAndTxIDs(
		map[string]map[string]uint64{"L": {"ref": 3}},
		map[string]map[string]uint64{"L": {"ref": 42}},
	)

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":                "L",
					"reference":             "ref",
					"existingTransactionId": "99", // audit-derived owner is 42
				},
			},
		},
	}

	events := captureEventsState(t, "L", 7, payload, expected, cb, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictPermissiveWhenOwnerUnknown pins the
// permissive fallback: when the owning tx id is not re-derivable (no entry in
// referenceTxIDs — e.g. a live claim on an unanchored/archived-CreateLedger
// ledger, or a purged claim with no baseline), the verifier must NOT pin
// existingTransactionId. Pinning it there would false-positive on a
// legitimate skip whose owner the checker genuinely cannot reconstruct.
func TestVerifySkippedOrder_ReferenceConflictPermissiveWhenOwnerUnknown(t *testing.T) {
	t.Parallel()

	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
			ledger:    "L",
			reference: "ref",
		},
	}
	// references seeded (so the firstSeenSeq < seq claim check passes) but
	// referenceTxIDs left empty → owner not derivable.
	cb := chainBoundStateFromRefsAndTxIDs(
		map[string]map[string]uint64{"L": {"ref": 3}},
		nil,
	)

	payload := &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				Context: map[string]string{
					"ledger":                "L",
					"reference":             "ref",
					"existingTransactionId": "7", // arbitrary — must NOT fail when owner unknown
				},
			},
		},
	}

	events := captureEventsState(t, "L", 7, payload, expected, cb, false)
	require.Empty(t, events, "must stay permissive on existingTransactionId when the owner is not re-derivable")
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
							SkippableReasons: skipReasons,
						},
					},
				},
			},
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

	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBoundStateFromRefs(refs))

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

	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBoundStateFromRefs(refs))

	require.Equal(t, uint64(50), refs["L"]["mirror-ref"],
		"mirror-ingested reference must be recorded at its log sequence so later skip verifiers see the prior claim")
}

// TestCollectExpectedSkippable_RemoveAccountTypeEmptyNameFlagsKind pins the
// checker-side half of finding checker.go:3472: a RemoveAccountType order
// with an empty name is still an AccountType order. collectExpectedSkippable
// must set isAccountTypeOrder so verifySkippedOrder discriminates on the
// action KIND, not on accountTypeName != "" — otherwise a legitimate
// ACCOUNT_TYPE_NOT_FOUND skip on the (degenerate) empty name is misclassified
// as "not an AccountType order" → INVALID_SKIP on a valid store.
func TestCollectExpectedSkippable_RemoveAccountTypeEmptyNameFlagsKind(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						SkippableReasons: []commonpb.ErrorReason{reason},
						Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{
							RemoveAccountType: &raftcmdpb.RemoveAccountTypeOrder{Name: ""},
						},
					},
				},
			},
		},
	}
	body, err := order.MarshalVT()
	require.NoError(t, err)

	items := []*auditpb.AuditItem{{SerializedOrder: body, LogSequence: 7}}
	chainBound := newChainBoundState()
	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBound)

	exp := expectedSkip[7]
	require.NotNil(t, exp)
	require.True(t, exp.isAccountTypeOrder,
		"a RemoveAccountType order — even with an empty name — must be flagged as an AccountType order")
	require.Empty(t, exp.accountTypeName)

	// The forward verifier must NOT reject this as "not an AccountType order":
	// the discriminant is the kind, not the non-empty name. (It may still emit
	// a different verdict from the presence check, but never the
	// misclassification error.)
	payload := skippedPayloadWithContext(reason, map[string]string{"name": ""})
	events := captureEventsState(t, "L", 7, payload, expectedSkip, chainBound, false)
	for _, e := range events {
		require.NotContains(t, e.GetError().GetMessage(), "is not an AccountType order",
			"empty-name RemoveAccountType must not be misclassified as a non-AccountType order")
	}
}

// buildCreateTxWithRefAndAccountMetaItem wraps a CreateTransactionOrder that
// declares a reference and carries account_metadata into a serialized audit
// item at the given log sequence.
func buildCreateTxWithRefAndAccountMetaItem(t *testing.T, ledger, reference, account, key string, logSeq uint64) *auditpb.AuditItem {
	t.Helper()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						// Whitelist CONFLICT so the create is skip-tolerant: the
						// archive-uncertain suppression only applies to conflict-
						// skippable creates (a non-skippable create hard-fails on a
						// prior claim, never reaching a success item).
						SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Reference: reference,
								AccountMetadata: map[string]*commonpb.MetadataMap{
									account: {Values: map[string]*commonpb.MetadataValue{key: commonpb.NewStringValue("v")}},
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := order.MarshalVT()
	require.NoError(t, err)

	return &auditpb.AuditItem{SerializedOrder: body, LogSequence: logSeq}
}

// TestRecordChainBoundMutations_ArchiveOnlyConflictDoesNotSeedAccountMetadata
// pins finding checker.go:2205: on an UNANCHORED ledger (no CreateLedger in
// the live range, no baseline fold) a CreateTransaction whose reference
// conflict is only provable from a purged chapter has an unprovable skip
// status — chainBoundCreateTxSkipped returns false because the prior claim
// is invisible. Its account_metadata must NOT be seeded as present, otherwise
// a later legitimate METADATA_NOT_FOUND skip on that key is false-positived
// as INVALID_SKIP. The create's application is unproven → the timeline stays
// silent and the skip stays permissive.
func TestRecordChainBoundMutations_ArchiveOnlyConflictDoesNotSeedAccountMetadata(t *testing.T) {
	t.Parallel()

	// Unanchored ledger: no CreateLedger item, no baseline seeding.
	items := []*auditpb.AuditItem{
		buildCreateTxWithRefAndAccountMetaItem(t, "L", "ref-1", "alice", "role", 50),
	}

	chainBound := newChainBoundState()
	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBound)

	// The account metadata timeline for (alice, role) must be empty: the
	// create's application could not be proven.
	require.Empty(t, chainBound.metadata["L"][metadataTimelineTarget(false, "alice")]["role"],
		"unprovable archive-only-conflict create must not seed account metadata as present")

	// A forged METADATA_NOT_FOUND skip at a later seq therefore stays
	// permissive (the key is not asserted present).
	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		60: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}
	payload := skippedPayloadWithContext(reason, map[string]string{"target": "alice", "key": "role"})
	events := captureEventsState(t, "L", 60, payload, expected, chainBound, false)
	require.Empty(t, events, "no false INVALID_SKIP when the create's application is unproven")
}

// TestRecordChainBoundMutations_AnchoredCreateSeedsAccountMetadata is the
// positive control: on an ANCHORED ledger (CreateLedger seen live) a
// CreateTransaction with an unclaimed reference provably applied, so its
// account_metadata IS seeded and a forged METADATA_NOT_FOUND on that key is
// correctly rejected.
func TestRecordChainBoundMutations_AnchoredCreateSeedsAccountMetadata(t *testing.T) {
	t.Parallel()

	createLedgerOrder := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger:  "L",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
			},
		},
	}
	clBody, err := createLedgerOrder.MarshalVT()
	require.NoError(t, err)

	items := []*auditpb.AuditItem{
		{SerializedOrder: clBody, LogSequence: 10},
		buildCreateTxWithRefAndAccountMetaItem(t, "L", "ref-1", "alice", "role", 50),
	}

	chainBound := newChainBoundState()
	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBound)

	require.NotEmpty(t, chainBound.metadata["L"][metadataTimelineTarget(false, "alice")]["role"],
		"anchored create with an unclaimed reference provably applied → account metadata seeded")

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		60: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}
	payload := skippedPayloadWithContext(reason, map[string]string{"target": "alice", "key": "role"})
	events := captureEventsState(t, "L", 60, payload, expected, chainBound, false)
	requireInvalidSkipEvent(t, events, 60)
}

// buildMirrorCreatedTxItem wraps a MirrorCreatedTransaction into a serialized
// audit item at the given log sequence, mirroring the shape
// collectExpectedSkippable decodes.
func buildMirrorCreatedTxItem(t *testing.T, ledger string, mct *raftcmdpb.MirrorCreatedTransaction, logSeq uint64) *auditpb.AuditItem {
	t.Helper()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{
						Entry: &raftcmdpb.MirrorLogEntry{
							Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
								CreatedTransaction: mct,
							},
						},
					},
				},
			},
		},
	}

	body, err := order.MarshalVT()
	require.NoError(t, err)

	return &auditpb.AuditItem{SerializedOrder: body, LogSequence: logSeq}
}

// TestVerifySkippedOrder_MirrorCreatedTxMetadataSeedsTimeline pins finding
// checker.go:2377: a mirror-ingested transaction that carries tx-scoped and
// account metadata has those keys APPLIED by processMirrorCreatedTransaction
// (TransactionState.Metadata + AccountMetadata().Put). recordMirrorIngestMutations
// must seed chainBound.metadata for them so a later forged
// METADATA_NOT_FOUND skip on such a key is caught (invariant #8). Previously
// only the reference was seeded, so the forged skip passed on an empty
// metadata timeline.
func TestVerifySkippedOrder_MirrorCreatedTxMetadataSeedsTimeline(t *testing.T) {
	t.Parallel()

	mct := &raftcmdpb.MirrorCreatedTransaction{
		TransactionId: 7,
		Reference:     "mirror-ref",
		Metadata: map[string]*commonpb.MetadataValue{
			"txkey": commonpb.NewStringValue("v"),
		},
		AccountMetadata: map[string]*commonpb.MetadataMap{
			"alice": {Values: map[string]*commonpb.MetadataValue{"acckey": commonpb.NewStringValue("v")}},
		},
	}

	items := []*auditpb.AuditItem{buildMirrorCreatedTxItem(t, "L", mct, 50)}

	// Two forged METADATA_NOT_FOUND skips at seq 60 (> 50): one on the
	// account key, one on the tx-scoped key. Both were really applied by the
	// mirror ingest, so both must be rejected.
	cases := []struct {
		name   string
		target string
		isTx   bool
		key    string
	}{
		{"account metadata key", "alice", false, "acckey"},
		{"tx-scoped metadata key", "7", true, "txkey"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			chainBound := newChainBoundState()
			expectedSkip := make(map[uint64]*expectedSkippableOrder)
			collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBound)

			reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
			expected := map[uint64]*expectedSkippableOrder{
				60: {
					reasons:            []commonpb.ErrorReason{reason},
					ledger:             "L",
					metadataTarget:     tc.target,
					metadataKey:        tc.key,
					metadataTargetIsTx: tc.isTx,
				},
			}

			payload := skippedPayloadWithContext(reason, map[string]string{
				"target": tc.target,
				"key":    tc.key,
			})

			events := captureEventsState(t, "L", 60, payload, expected, chainBound, false)
			requireInvalidSkipEvent(t, events, 60)
		})
	}
}

// TestVerifySkippedOrder_MirrorCreatedTxUnrelatedKeyStillSkippable is the
// legitimate companion: a METADATA_NOT_FOUND skip on a key the mirror
// transaction did NOT carry is genuinely absent and stays accepted.
func TestVerifySkippedOrder_MirrorCreatedTxUnrelatedKeyStillSkippable(t *testing.T) {
	t.Parallel()

	mct := &raftcmdpb.MirrorCreatedTransaction{
		TransactionId: 7,
		Reference:     "mirror-ref",
		AccountMetadata: map[string]*commonpb.MetadataMap{
			"alice": {Values: map[string]*commonpb.MetadataValue{"acckey": commonpb.NewStringValue("v")}},
		},
	}

	items := []*auditpb.AuditItem{buildMirrorCreatedTxItem(t, "L", mct, 50)}

	chainBound := newChainBoundState()
	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBound)

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	// Different key ("other") on the same account — never applied by the
	// mirror ingest, so the delete legitimately skips NOT_FOUND.
	expected := map[uint64]*expectedSkippableOrder{
		60: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "other",
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "other",
	})

	events := captureEventsState(t, "L", 60, payload, expected, chainBound, false)
	require.Empty(t, events, "a key the mirror tx never carried is genuinely absent → legitimate skip")
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
							SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
						},
					},
				},
			},
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

	collectExpectedSkippable(items, 1, ^uint64(0), expectedSkip, chainBoundStateFromRefs(refs))

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

	return captureEventsState(t, ledger, seq, payload, expected, chainBoundStateFromRefs(refs), hasArchivedChapters)
}

// captureEventsState is captureEvents' extended variant: tests that need
// to configure the reverted / metadata / accountTypes maps build a
// *chainBoundState directly (via chainBoundStateWith) instead of the
// refs-only shortcut. The reference-conflict tests keep the shorter API.
func captureEventsState(
	t *testing.T,
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expected map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
	hasArchivedChapters bool,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	events := []*servicepb.CheckStoreEvent{}

	// Baseline default: assume neither the baseline reference set nor
	// the baseline chain-state fold ran. Tests that need to exercise
	// the baseline-covered paths build the state directly and call
	// verifySkippedOrder inline with the appropriate flags.
	verifySkippedOrder(ledger, seq, payload, expected, chainBound, hasArchivedChapters, false, false, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	return events
}

// chainBoundStateFromRefs builds a *chainBoundState with only the
// references map populated — every other timeline is empty. Kept for
// the existing reference-conflict test set; new tests that need to
// exercise the other reasons build their own state directly.
func chainBoundStateFromRefs(refs map[string]map[string]uint64) *chainBoundState {
	cb := newChainBoundState()
	if refs != nil {
		cb.references = refs
	}

	return cb
}

// chainBoundStateFromRefsAndTxIDs builds a *chainBoundState with the
// references (ledger → reference → firstSeenSeq) and referenceTxIDs
// (ledger → reference → owning tx id) maps populated. Used by the
// existingTransactionId verification tests; a nil txIDs argument leaves
// the owner-lookup empty (permissive path).
func chainBoundStateFromRefsAndTxIDs(
	refs map[string]map[string]uint64,
	txIDs map[string]map[string]uint64,
) *chainBoundState {
	cb := chainBoundStateFromRefs(refs)
	if txIDs != nil {
		cb.referenceTxIDs = txIDs
	}

	return cb
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

	dispatchElisionCheck(seq, log, expected, chainBoundStateFromRefs(refs), false, false, func(e *servicepb.CheckStoreEvent) {
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

// skippedPayloadWithContext builds an OrderSkipped payload with the given
// reason and a fully-populated context map — used by the new-reason
// verifier tests to exercise both the accept and tamper paths without
// duplicating the payload constructor per case.
func skippedPayloadWithContext(reason commonpb.ErrorReason, ctx map[string]string) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{
				Reason:  reason,
				Context: ctx,
			},
		},
	}
}

// TestVerifySkippedOrder_RevertAlreadyRevertedAcceptsMatchingContext pins
// the happy path for TRANSACTION_ALREADY_REVERTED: the projection carries
// the same transaction id the chain-bound RevertTransactionOrder
// targeted AND chainBound.reverted shows an earlier successful revert of
// that same tx (the audit-derived proof that a skip was legitimate).
func TestVerifySkippedOrder_RevertAlreadyRevertedAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:       []commonpb.ErrorReason{reason},
			ledger:        "L",
			transactionID: 42,
		},
	}

	chainBound := newChainBoundState()
	chainBound.reverted["L"] = map[uint64]uint64{42: 3} // reverted at seq 3, well before 7

	payload := skippedPayloadWithContext(reason, map[string]string{"transactionId": "42"})
	events := captureEventsState(t, "L", 7, payload, expected, chainBound, false)
	require.Empty(t, events, "matching correlator + earlier revert must not emit any INVALID_SKIP")
}

// TestVerifySkippedOrder_RevertAlreadyRevertedRejectsWithoutEarlierRevert
// closes the log-only tampering vector: even with a matching correlator,
// a skip is illegitimate unless the audit chain shows a prior successful
// revert of the same tx. Empty chainBound.reverted → INVALID_SKIP.
func TestVerifySkippedOrder_RevertAlreadyRevertedRejectsWithoutEarlierRevert(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:       []commonpb.ErrorReason{reason},
			ledger:        "L",
			transactionID: 42,
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"transactionId": "42"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RevertAlreadyRevertedRejectsMissingCorrelator
// catches a projection that records the reason on an order the chain
// says is NOT a RevertTransactionOrder — expected.transactionID stays 0
// and the verifier surfaces the mismatch.
func TestVerifySkippedOrder_RevertAlreadyRevertedRejectsMissingCorrelator(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{reason}, ledger: "L"},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"transactionId": "42"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_RevertAlreadyRevertedRejectsTamperedTxID catches
// a projection whose context.transactionId was flipped to hide the
// actual target of the double-revert.
func TestVerifySkippedOrder_RevertAlreadyRevertedRejectsTamperedTxID(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:       []commonpb.ErrorReason{reason},
			ledger:        "L",
			transactionID: 42,
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"transactionId": "13"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundAcceptsMatchingContext pins the
// happy path for METADATA_NOT_FOUND on DeleteMetadata: matching context
// + chain-bound state shows the key was ABSENT just before seq. The
// default absent state (empty metadata timeline) is what a legitimate
// "delete key that never existed" skip looks like.
func TestVerifySkippedOrder_MetadataNotFoundAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}

	// Empty metadata timeline → key absent by default at seq 7.
	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "role",
	})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_MetadataNotFoundRejectsWhenKeyWasPresent closes
// the log-only tampering vector: if the chain shows a Set for the key
// at an earlier seq (with no subsequent Delete), the key WAS present
// just before seq — a legitimate Delete would have succeeded, not
// skipped. Marks the projection as INVALID_SKIP.
func TestVerifySkippedOrder_MetadataNotFoundRejectsWhenKeyWasPresent(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}

	chainBound := newChainBoundState()
	chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{
		metadataTimelineTarget(false, "alice"): {"role": {{seq: 3, exists: true}}}, // Set at 3, still present at 7
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "role",
	})
	events := captureEventsState(t, "L", 7, payload, expected, chainBound, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundRejectsTamperedKey catches a
// projection whose context.key was flipped — the tampering vector where
// the caller thinks they deleted "role" but the chain-bound order was
// actually deleting "team".
func TestVerifySkippedOrder_MetadataNotFoundRejectsTamperedKey(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "team",
	})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundRejectsTamperedTarget catches a
// projection whose context.target was flipped (e.g. account address
// swapped for a different account).
func TestVerifySkippedOrder_MetadataNotFoundRejectsTamperedTarget(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice",
			metadataKey:    "role",
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "bob",
		"key":    "role",
	})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundRejectsMissingCorrelator pins
// the "wrong order kind" case: a METADATA_NOT_FOUND skip on an order the
// chain says isn't a DeleteMetadataOrder.
func TestVerifySkippedOrder_MetadataNotFoundRejectsMissingCorrelator(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{reason}, ledger: "L"},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"target": "alice", "key": "role"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_AccountTypeAlreadyExistsAcceptsMatchingContext.
// Happy path: matching context + chain shows the account type was
// PRESENT just before seq (added earlier, not since removed).
func TestVerifySkippedOrder_AccountTypeAlreadyExistsAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	chainBound := newChainBoundState()
	chainBound.accountTypes["L"] = map[string][]chainBoundMutation{
		"customer": {{seq: 3, exists: true}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	events := captureEventsState(t, "L", 7, payload, expected, chainBound, false)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsWhenAbsent closes
// the log-only tampering vector: without a prior Add, the "ALREADY_EXISTS"
// skip claim is inconsistent with the audit chain.
func TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsWhenAbsent(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsTamperedName
// catches a projection whose context.name was flipped.
func TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsTamperedName(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "vendor"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_AccountTypeNotFoundAcceptsMatchingContext
// exercises the symmetric case for the RemoveAccountType path: matching
// context + chain shows the account type was ABSENT just before seq
// (empty timeline, or last mutation was a Remove).
func TestVerifySkippedOrder_AccountTypeNotFoundAcceptsMatchingContext(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	// Empty accountTypes timeline → absent by default.
	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	require.Empty(t, events)
}

// TestVerifySkippedOrder_AccountTypeNotFoundRejectsWhenPresent closes the
// log-only tampering vector for the NOT_FOUND direction: chain shows a
// prior Add without a subsequent Remove → the name WAS present, so a
// legitimate Remove would have succeeded, not skipped.
func TestVerifySkippedOrder_AccountTypeNotFoundRejectsWhenPresent(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	chainBound := newChainBoundState()
	chainBound.accountTypes["L"] = map[string][]chainBoundMutation{
		"customer": {{seq: 3, exists: true}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	events := captureEventsState(t, "L", 7, payload, expected, chainBound, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_AccountTypeNotFoundRejectsMissingCorrelator pins
// the "wrong order kind" case for AccountType reasons.
func TestVerifySkippedOrder_AccountTypeNotFoundRejectsMissingCorrelator(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {reasons: []commonpb.ErrorReason{reason}, ledger: "L"},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	events := captureEvents(t, "L", 7, payload, expected, nil, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestCollectExpectedSkippable_TracksTransactionScopedMetadata pins the
// per-ledger nextTxID counter: metadata written by CreateTransaction
// (via .metadata → new tx id) and RevertTransaction (via .metadata →
// new revert tx id) MUST appear on chainBound.metadata at target=<tx
// id string>. Without this a later DeleteMetadata(target=TransactionId,
// key=X) with skippable_reasons=[METADATA_NOT_FOUND] would falsely pass
// the "was absent" check because the timeline would look empty.
func TestCollectExpectedSkippable_TracksTransactionScopedMetadata(t *testing.T) {
	t.Parallel()

	// Build a synthetic audit-item stream on ledger "L":
	//   seq=10: CreateLedger — seeds nextTxID to 1
	//   seq=11: CreateTransaction with metadata{foo:bar} — takes tx id 1
	//   seq=12: CreateTransaction with reference "ref-A" — takes tx id 2
	//   seq=13: CreateTransaction with reference "ref-A" AND skippable_reasons=[REF_CONFLICT]
	//           → skipped (chain shows ref-A claimed at 12), does NOT consume tx id 3
	//   seq=14: RevertTransaction targeting tx id 1 with metadata{note:reverted}
	//           → NEW tx id 3 assigned to the revert
	//
	// After the run, chainBound.metadata["L"] must contain:
	//   target="1", key="foo": {seq=11, exists=true}
	//   target="3", key="note": {seq=14, exists=true}
	// AND chainBound.nextTxID["L"] must be 4.

	items := []*auditpb.AuditItem{
		buildAuditItem(t, 10, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		}}),
		buildAuditItem(t, 11, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Metadata: map[string]*commonpb.MetadataValue{"foo": commonpb.NewStringValue("bar")},
						},
					}},
				},
			},
		}}),
		buildAuditItem(t, 12, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{Reference: "ref-A"},
					}},
				},
			},
		}}),
		buildAuditItem(t, 13, &raftcmdpb.Order{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "L",
					Payload: &raftcmdpb.LedgerScopedOrder_Apply{
						Apply: &raftcmdpb.LedgerApplyOrder{
							Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Reference: "ref-A",
									Metadata:  map[string]*commonpb.MetadataValue{"skipped": commonpb.NewStringValue("y")},
									AccountMetadata: map[string]*commonpb.MetadataMap{
										"alice": {Values: map[string]*commonpb.MetadataValue{"skipped-acct": commonpb.NewStringValue("z")}},
									},
								},
							},
							SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
						},
					},
				},
			},
		}),
		buildAuditItem(t, 14, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
						RevertTransaction: &raftcmdpb.RevertTransactionOrder{
							TransactionId: 1,
							Metadata:      map[string]*commonpb.MetadataValue{"note": commonpb.NewStringValue("reverted")},
						},
					}},
				},
			},
		}}),
	}

	cb := newChainBoundState()
	collectExpectedSkippable(items, 1, ^uint64(0), map[uint64]*expectedSkippableOrder{}, cb)

	// Tx-scoped metadata from CreateTransaction @ seq=11 → target="1".
	fooMuts := cb.metadata["L"]["1"]["foo"]
	require.Len(t, fooMuts, 1)
	require.True(t, fooMuts[0].exists, "CreateTransaction.metadata must be recorded as exists=true")
	require.Equal(t, uint64(11), fooMuts[0].seq)

	// Tx-scoped metadata from RevertTransaction @ seq=14 → target="3"
	// (revert consumes a new tx id after ref-A at 12 was assigned 2).
	noteMuts := cb.metadata["L"]["3"]["note"]
	require.Len(t, noteMuts, 1)
	require.True(t, noteMuts[0].exists)
	require.Equal(t, uint64(14), noteMuts[0].seq)

	// Skipped CreateTransaction @ seq=13 did NOT consume tx id 3 and
	// did NOT leak its metadata onto the timeline.
	require.Empty(t, cb.metadata["L"]["3"]["skipped"], "skipped CreateTransaction.metadata must not surface on the timeline")

	// A skipped CreateTransaction never applies its account_metadata either
	// (the sub-processor returns the conflict error before any Put), so it
	// must not fabricate a presence for (account=alice, key=skipped-acct) —
	// otherwise a later legitimate DeleteMetadata(METADATA_NOT_FOUND) on
	// that key would be false-flagged as INVALID_SKIP.
	require.Empty(t, cb.metadata["L"]["alice"]["skipped-acct"], "skipped CreateTransaction.account_metadata must not surface on the timeline")

	// Counter: 1 (initial) + create(1→2) + create(2→3) + skip + revert(3→4) = 4.
	require.Equal(t, uint64(4), cb.nextTxID["L"], "nextTxID must reflect only successful CreateTransaction/RevertTransaction orders")
}

// buildAuditItem is the test-side constructor for an AuditItem: takes
// the log seq and the raftcmdpb.Order to serialize into serialized_order.
func buildAuditItem(t *testing.T, logSeq uint64, order *raftcmdpb.Order) *auditpb.AuditItem {
	t.Helper()

	raw, err := order.MarshalVT()
	require.NoError(t, err)

	return &auditpb.AuditItem{
		LogSequence:     logSeq,
		SerializedOrder: raw,
	}
}

// TestVerifySkippedOrder_LedgerMismatchAcrossReasons pins the hoisted
// ledger-envelope check: for EVERY whitelisted reason (not just
// CONFLICT), a projection that claims a different ledger than the
// chain-bound order MUST be flagged. Log sequences are proposal-global,
// so exactly one chain-bound order sits at each seq — a projection at
// a different ledger identifies the tampering directly.
func TestVerifySkippedOrder_LedgerMismatchAcrossReasons(t *testing.T) {
	t.Parallel()

	// One case per whitelisted reason to prove the check is uniform
	// across the switch — before the fix, only CONFLICT would fire.
	cases := []struct {
		name       string
		reason     commonpb.ErrorReason
		expected   *expectedSkippableOrder
		context    map[string]string
		buildBound func(*chainBoundState)
	}{
		{
			name:   "revert_already_reverted",
			reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED,
			expected: &expectedSkippableOrder{
				reasons:       []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED},
				ledger:        "audit-L",
				transactionID: 42,
			},
			context: map[string]string{"transactionId": "42"},
			buildBound: func(cb *chainBoundState) {
				cb.reverted["audit-L"] = map[uint64]uint64{42: 3}
			},
		},
		{
			name:   "metadata_not_found",
			reason: commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND,
			expected: &expectedSkippableOrder{
				reasons:        []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND},
				ledger:         "audit-L",
				metadataTarget: "alice",
				metadataKey:    "role",
			},
			context: map[string]string{"target": "alice", "key": "role"},
		},
		{
			name:   "account_type_already_exists",
			reason: commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS,
			expected: &expectedSkippableOrder{
				reasons:            []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS},
				ledger:             "audit-L",
				accountTypeName:    "customer",
				isAccountTypeOrder: true,
			},
			context: map[string]string{"name": "customer"},
			buildBound: func(cb *chainBoundState) {
				cb.accountTypes["audit-L"] = map[string][]chainBoundMutation{
					"customer": {{seq: 3, exists: true}},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			expected := map[uint64]*expectedSkippableOrder{7: tc.expected}

			cb := newChainBoundState()
			if tc.buildBound != nil {
				tc.buildBound(cb)
			}

			// Projection claims "tampered-L" while the chain-bound
			// order targets "audit-L". Every reason must fire.
			payload := skippedPayloadWithContext(tc.reason, tc.context)
			events := captureEventsState(t, "tampered-L", 7, payload, expected, cb, false)
			requireInvalidSkipEvent(t, events, 7)
		})
	}
}

// TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsWhenLiveRemoved
// closes the archive-escape hole where a live RemoveAccountType before
// seq is positive proof of absence — archives cannot undo a live
// removal, so the escape must NOT apply. Live-witnessed absence is
// authoritative.
func TestVerifySkippedOrder_AccountTypeAlreadyExistsRejectsWhenLiveRemoved(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	cb := newChainBoundState()
	cb.accountTypes["L"] = map[string][]chainBoundMutation{
		"customer": {
			{seq: 2, exists: true},  // Add
			{seq: 5, exists: false}, // Remove — live-witnessed absence at seq 7
		},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})
	// hasArchivedChapters=true — the escape must NOT trigger because
	// live has positive evidence of absence via the Remove at seq 5.
	events := captureEventsState(t, "L", 7, payload, expected, cb, true)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundPermissiveOnArchivedTxIDLedger
// pins the "archived-history ledger + tx-id target" safety net: when
// the ledger's CreateLedger was NOT observed in live (chain scan opened
// past the ledger's history), the nextTxID counter is defaulted and
// tx-scoped metadata was NOT recorded. The verifier stays permissive
// for tx-id-scoped METADATA_NOT_FOUND skips under archives on such
// ledgers — a strict check would produce a false-positive INVALID_SKIP
// on legitimate skips. Account-address targets remain strictly
// verified because their recording bypasses the counter.
func TestVerifySkippedOrder_MetadataNotFoundPermissiveOnArchivedTxIDLedger(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			metadataTarget:     "101", // tx-id-shaped target
			metadataKey:        "foo",
			metadataTargetIsTx: true,
		},
	}

	// Bare state: ledgerCreationSeen is empty (archived history), no
	// metadata recorded for target="101". Under archives the tx-id
	// verification should stay permissive.
	cb := newChainBoundState()

	payload := skippedPayloadWithContext(reason, map[string]string{"target": "101", "key": "foo"})
	events := captureEventsState(t, "L", 7, payload, expected, cb, true)
	require.Empty(t, events, "tx-id target on archived-history ledger must be permissive")

	// Contrast: same setup WITHOUT archives should NOT stay permissive
	// — my check would otherwise silently hide real forgeries on
	// live-only ledgers.
	events = captureEventsState(t, "L", 7, payload, expected, cb, false)
	// present=false, no fire on "was present". Falls through with no
	// error. Non-archives behavior is unchanged from the pre-fix
	// baseline for empty timelines.
	require.Empty(t, events)
}

// TestCollectExpectedSkippable_LegacyReplayReferenceFoldedOnce pins the
// upgrade-compat fix for finding checker.go:2100. Before the per-batch
// idempotency commit (f9ee1e829), an idempotent replay emitted a per-order
// ReferenceSequence that buildAuditItems persisted into AuditItem.LogSequence,
// and the success path STILL wrote an audit entry — so an upgraded store can
// hold chain-verified AuditItems that back-reference an earlier entry's log.
// collectExpectedSkippable must fold each referenced log ONCE: the legacy
// replay item (LogSequence outside this entry's [min,max]) is skipped, so it
// does not double-bump nextTxID or re-seed the metadata timeline.
func TestCollectExpectedSkippable_LegacyReplayReferenceFoldedOnce(t *testing.T) {
	t.Parallel()

	// A single legacy audit entry: the fresh CreateTransaction (log seq 10,
	// inside [min,max]=[10,10]) plus a legacy per-order replay item carrying
	// the SAME serialized order but referencing an EARLIER log (seq 5, outside
	// [10,10]). Pre-f9ee1e829 these coexisted in one entry.
	fresh := buildCreateTxWithRefAndAccountMetaItem(t, "L", "ref-1", "alice", "role", 10)
	legacyReplay := buildCreateTxWithRefAndAccountMetaItem(t, "L", "ref-1", "alice", "role", 5)
	items := []*auditpb.AuditItem{fresh, legacyReplay}

	// Anchored ledger so account_metadata seeds and nextTxID bumps
	// deterministically. Seed nextTxID=1 as CreateLedger would.
	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeen["L"] = struct{}{}
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}
	chainBound.nextTxID["L"] = 1

	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	// [min,max] = [10,10]: only the fresh item is in range; the replay
	// reference at seq 5 is below min and must be filtered.
	collectExpectedSkippable(items, 10, 10, expectedSkip, chainBound)

	// nextTxID advanced by exactly ONE (the fresh create), not two.
	require.Equal(t, uint64(2), chainBound.nextTxID["L"],
		"the legacy replay reference must not double-bump nextTxID")

	// The account metadata (alice, role) timeline has exactly ONE presence
	// entry (from the fresh create), not a duplicate from the replay.
	require.Len(t, chainBound.metadata["L"][metadataTimelineTarget(false, "alice")]["role"], 1,
		"the legacy replay reference must not re-seed the metadata timeline")

	// A forged METADATA_NOT_FOUND on (alice, role) — the real key the create
	// set — is rejected: the single seeded presence witness fires INVALID_SKIP.
	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		20: {reasons: []commonpb.ErrorReason{reason}, ledger: "L", metadataTarget: "alice", metadataKey: "role"},
	}
	payload := skippedPayloadWithContext(reason, map[string]string{"target": "alice", "key": "role"})
	events := captureEventsState(t, "L", 20, payload, expected, chainBound, false)
	requireInvalidSkipEvent(t, events, 20)
}

// buildCreateTxNonSkippableWithAccountMeta builds a CreateTransactionOrder
// that declares a reference and account_metadata but does NOT whitelist any
// skippable reason — modeling a create that can only succeed or hard-fail,
// never skip.
func buildCreateTxNonSkippableWithAccountMeta(t *testing.T, ledger, reference, account, key string, logSeq uint64) *auditpb.AuditItem {
	t.Helper()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						// No SkippableReasons: this create cannot be converted
						// to an OrderSkipped.
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Reference: reference,
								AccountMetadata: map[string]*commonpb.MetadataMap{
									account: {Values: map[string]*commonpb.MetadataValue{key: commonpb.NewStringValue("v")}},
								},
							},
						},
					},
				},
			},
		},
	}

	body, err := order.MarshalVT()
	require.NoError(t, err)

	return &auditpb.AuditItem{SerializedOrder: body, LogSequence: logSeq}
}

// TestRecordChainBoundMutations_NonConflictSkippableCreateSeedsUnconditionally
// pins finding checker.go:2295. A CreateTransaction that did NOT whitelist
// TRANSACTION_REFERENCE_CONFLICT cannot be converted to an OrderSkipped — if
// it hit a prior claim the FSM HARD-FAILS (no success item). So its presence
// in a success item proves it applied; the archive-uncertain suppression
// (which exists only to avoid asserting a skipped create's writes) must NOT
// drop its account_metadata even on an unanchored, archived ledger. Otherwise
// a later forged METADATA_NOT_FOUND on that account/key would pass as
// archive-inconclusive.
func TestRecordChainBoundMutations_NonConflictSkippableCreateSeedsUnconditionally(t *testing.T) {
	t.Parallel()

	// Unanchored, archive-only conditions (same as the conflict-skippable
	// suppression test) — but this create is NOT conflict-skippable.
	items := []*auditpb.AuditItem{
		buildCreateTxNonSkippableWithAccountMeta(t, "L", "ref-1", "alice", "role", 50),
	}

	chainBound := newChainBoundState()
	expectedSkip := make(map[uint64]*expectedSkippableOrder)
	collectExpectedSkippable(items, 50, 50, expectedSkip, chainBound)

	// account_metadata IS seeded (the create provably applied).
	require.NotEmpty(t, chainBound.metadata["L"][metadataTimelineTarget(false, "alice")]["role"],
		"a non-conflict-skippable create present in a success item provably applied → seed its account metadata")

	// A forged METADATA_NOT_FOUND on (alice, role) is therefore rejected.
	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		60: {reasons: []commonpb.ErrorReason{reason}, ledger: "L", metadataTarget: "alice", metadataKey: "role"},
	}
	payload := skippedPayloadWithContext(reason, map[string]string{"target": "alice", "key": "role"})
	events := captureEventsState(t, "L", 60, payload, expected, chainBound, false)
	requireInvalidSkipEvent(t, events, 60)
}
