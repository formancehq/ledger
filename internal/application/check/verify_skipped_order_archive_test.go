package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// captureEventsArchive calls verifySkippedOrder with explicit archive /
// baseline flags AND a caller-controlled *chainBoundState, so tests can pin
// the interaction between the archive escape, the baseline folds, and the
// live-created-ledger marker (chainBound.ledgerCreationSeenLive). The
// shared captureEvents/captureEventsState helpers hardcode both baseline
// flags to false, which is insufficient for these paths.
func captureEventsArchive(
	t *testing.T,
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expected map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
	hasArchivedChapters bool,
	baselineReferencesAvailable bool,
	baselineChainStateAvailable bool,
) []*servicepb.CheckStoreEvent {
	t.Helper()

	events := []*servicepb.CheckStoreEvent{}
	verifySkippedOrder(ledger, seq, payload, expected, chainBound, hasArchivedChapters, baselineReferencesAvailable, baselineChainStateAvailable, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	return events
}

// TestVerifySkippedOrder_ReferenceConflictArchiveEscapeRejectedForLiveLedger
// pins finding 2adbf685cc for TRANSACTION_REFERENCE_CONFLICT: the archive
// escape (no live claim + archived chapters + no baseline references) must
// NOT fire when the conflicting ledger's CreateLedger was observed in the
// live audit range. A live-created ledger has its entire history live, so a
// missing reference claim proves the skip is forged, not archived.
func TestVerifySkippedOrder_ReferenceConflictArchiveEscapeRejectedForLiveLedger(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{reason},
			ledger:    "L",
			reference: "ref-1",
		},
	}

	// No prior claim of ref-1 in the live range; archived chapters exist and
	// the baseline reference fold did NOT run.
	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"reference": "ref-1",
		"ledger":    "L",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true /*archived*/, false /*noBaselineRefs*/, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_ReferenceConflictArchiveEscapeAppliesForArchivedLedger
// is the companion: the SAME configuration but WITHOUT a live CreateLedger
// stays permissive — the claim may live in a purged chapter, so the checker
// cannot prove tampering.
func TestVerifySkippedOrder_ReferenceConflictArchiveEscapeAppliesForArchivedLedger(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:   []commonpb.ErrorReason{reason},
			ledger:    "L",
			reference: "ref-1",
		},
	}

	chainBound := newChainBoundState() // no ledgerCreationSeenLive entry

	payload := skippedPayloadWithContext(reason, map[string]string{
		"reference": "ref-1",
		"ledger":    "L",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "archived ledger without live creation stays permissive")
}

// TestVerifySkippedOrder_AlreadyRevertedArchiveEscapeRejectedForLiveLedger
// pins the same live-created-ledger gate for TRANSACTION_ALREADY_REVERTED.
func TestVerifySkippedOrder_AlreadyRevertedArchiveEscapeRejectedForLiveLedger(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:       []commonpb.ErrorReason{reason},
			ledger:        "L",
			transactionID: 42,
		},
	}

	// No earlier revert of tx 42 in the live range; archived chapters exist,
	// baseline chain state fold did NOT run, but the ledger was created live.
	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}

	payload := skippedPayloadWithContext(reason, map[string]string{"transactionId": "42"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundNumericAccountNotEscapedAsTx pins
// finding 48237506b7a54e57: an account whose ADDRESS is the numeric string
// "123" must NOT be granted the tx-id-only archive escape (which only exists
// for genuine transaction-id targets on unanchored ledgers). Keying the
// escape off metadataTargetIsTx — the preserved target kind — instead of the
// numeric-looking string closes the vector where a forged METADATA_NOT_FOUND
// on a numeric account passes on an archived ledger.
func TestVerifySkippedOrder_MetadataNotFoundNumericAccountNotEscapedAsTx(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			metadataTarget:     "123", // account address that LOOKS like a tx id
			metadataKey:        "role",
			metadataTargetIsTx: false, // it is an ACCOUNT, not a tx id
		},
	}

	// The account "123" had metadata "role" SET live and never deleted, so a
	// DeleteMetadata would have SUCCEEDED, not skipped NOT_FOUND. The ledger
	// is unanchored (no live CreateLedger) and archived chapters exist — the
	// old string-based isNumericTxIDTarget escape would have wrongly returned
	// permissively here.
	chainBound := newChainBoundState()
	chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{
		"123": {"role": {{seq: 3, exists: true}}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "123",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundTxTargetStillEscapesOnUnanchored
// confirms the tx-id-only escape STILL applies for a genuine transaction-id
// target on an unanchored (archived-CreateLedger) ledger — the fix narrows
// the escape to real tx targets, it does not remove it.
func TestVerifySkippedOrder_MetadataNotFoundTxTargetStillEscapesOnUnanchored(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			metadataTarget:     "123", // genuine transaction id
			metadataKey:        "role",
			metadataTargetIsTx: true,
		},
	}

	// Unanchored ledger (no live CreateLedger), archived chapters exist: the
	// tx-scoped metadata timeline is unreliable, so verification is
	// inconclusive → permissive.
	chainBound := newChainBoundState()

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "123",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "tx-id target on unanchored archived ledger stays permissive")
}

// TestVerifySkippedOrder_MetadataNotFoundTxTargetLiveWitnessBeatsArchiveEscape
// pins finding checker.go:3335: a live SetMetadata/SaveMetadata for a tx-id
// target is recorded directly from the order (independent of the archived
// nextTxID counter), so it is a trustworthy presence witness even on an
// unanchored ledger. The tx-id archive escape must NOT fire when such a live
// witness exists — a forged METADATA_NOT_FOUND on a (txID, key) the live
// chain proves present must be caught (invariant #8).
func TestVerifySkippedOrder_MetadataNotFoundTxTargetLiveWitnessBeatsArchiveEscape(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			metadataTarget:     "123", // genuine transaction id
			metadataKey:        "role",
			metadataTargetIsTx: true,
		},
	}

	// Unanchored ledger (no live CreateLedger) + archived chapters, BUT a live
	// SetMetadata for (tx 123, "role") at seq 3 recorded a presence witness.
	// The old early-return escape discarded this witness and let the forged
	// skip pass; the fix consults the live timeline first.
	chainBound := newChainBoundState()
	chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{
		"123": {"role": {{seq: 3, exists: true}}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "123",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_MetadataNotFoundTxTargetLiveDeleteWitnessAccepted is
// the legitimate companion: a live SetMetadata followed by a live
// DeleteMetadata for the same (txID, key) before seq means the key was
// absent at seq, so a METADATA_NOT_FOUND skip is genuine and accepted — the
// witness proves absence, the archive escape is not needed and not fired.
func TestVerifySkippedOrder_MetadataNotFoundTxTargetLiveDeleteWitnessAccepted(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			metadataTarget:     "123",
			metadataKey:        "role",
			metadataTargetIsTx: true,
		},
	}

	chainBound := newChainBoundState()
	chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{
		"123": {"role": {{seq: 3, exists: true}, {seq: 5, exists: false}}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "123",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "live delete witness proves absence → legitimate skip accepted")
}

// TestVerifySkippedOrder_AccountTypeNotFoundInconclusiveWhenArchivedNoBaseline
// pins finding checker.go:3364: an empty accountTypes timeline is NOT sound
// proof-of-absence when the ledger's whole history may be archived and the
// baseline was not folded. The skip stays permissive (inconclusive) rather
// than being validated as absence.
func TestVerifySkippedOrder_AccountTypeNotFoundInconclusiveWhenArchivedNoBaseline(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:         []commonpb.ErrorReason{reason},
			ledger:          "L",
			accountTypeName: "customer",
		},
	}

	// Empty accountTypes timeline; archived chapters exist; baseline fold did
	// NOT run; ledger NOT created live → absence cannot be proven.
	chainBound := newChainBoundState()

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "unprovable absence under archive-without-baseline stays permissive")
}

// TestVerifySkippedOrder_AccountTypeNotFoundProvenAbsentForLiveLedger is the
// companion: on a LIVE-created ledger an empty accountTypes timeline IS
// positive proof the type was never added, so a NOT_FOUND skip is legitimate
// and accepted (no inconclusive escape needed).
func TestVerifySkippedOrder_AccountTypeNotFoundProvenAbsentForLiveLedger(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:         []commonpb.ErrorReason{reason},
			ledger:          "L",
			accountTypeName: "customer",
		},
	}

	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "empty timeline on live-created ledger is proven absence → accepted")
}

// TestVerifySkippedOrder_AccountTypeNotFoundBaselineFoldedPresenceRejects
// pins the caught-forgery path for finding checker.go:3364: when the baseline
// fold DID run and folded the account type as present (sentinel seq 0), a
// forged NOT_FOUND skip is rejected because the type actually existed.
func TestVerifySkippedOrder_AccountTypeNotFoundBaselineFoldedPresenceRejects(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:         []commonpb.ErrorReason{reason},
			ledger:          "L",
			accountTypeName: "customer",
		},
	}

	// foldBaselineLedgers seeds account types present at ledger creation with
	// sentinel seq 0. The type IS present just before seq 7, so a
	// RemoveAccountType would have SUCCEEDED — the NOT_FOUND skip is forged.
	chainBound := newChainBoundState()
	chainBound.accountTypes["L"] = map[string][]chainBoundMutation{
		"customer": {{seq: 0, exists: true}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, true /*baselineChainState*/)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_AccountTypeAlreadyExistsForgedOnLiveLedgerRejected
// pins the ALREADY_EXISTS live-ledger tightening (finding 2adbf685cc): an
// empty timeline on a live-created ledger proves the type never existed, so
// a forged ALREADY_EXISTS skip is caught even under archived chapters.
func TestVerifySkippedOrder_AccountTypeAlreadyExistsForgedOnLiveLedgerRejected(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:         []commonpb.ErrorReason{reason},
			ledger:          "L",
			accountTypeName: "customer",
		},
	}

	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	requireInvalidSkipEvent(t, events, 7)
}
