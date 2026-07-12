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

// TestVerifySkippedOrder_MetadataNotFoundNumericAccountWitnessedPresenceRejected
// pins finding 48237506b7a54e57: an account whose ADDRESS is the numeric
// string "123" must NOT be able to hide a forged METADATA_NOT_FOUND behind
// the archive escape. The real guard is the `present` check: a live Set of
// the key makes present=true regardless of target kind, so the forged skip is
// caught even on an unanchored, archived ledger.
func TestVerifySkippedOrder_MetadataNotFoundNumericAccountWitnessedPresenceRejected(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "123", // account address that LOOKS like a tx id
			metadataKey:    "role",
		},
	}

	// The account "123" had metadata "role" SET live and never deleted, so a
	// DeleteMetadata would have SUCCEEDED, not skipped NOT_FOUND. The ledger
	// is unanchored (no live CreateLedger) and archived chapters exist — the
	// witnessed live presence must still reject the forged skip.
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

// TestVerifySkippedOrder_MetadataNotFoundAccountArchiveInconclusiveStaysPermissive
// pins the account-target half of finding checker.go:3466: on an unanchored,
// archived ledger with NO live witness and no baseline, an account-metadata
// key's Set may live only in a purged chapter — absence is unprovable, so the
// skip stays permissive (symmetric with the tx-target case).
func TestVerifySkippedOrder_MetadataNotFoundAccountArchiveInconclusiveStaysPermissive(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "alice", // account address, empty timeline
			metadataKey:    "role",
		},
	}

	// Unanchored ledger (no live CreateLedger), archived chapters, empty live
	// timeline for (alice, role): the Set could live only in a purged chapter.
	chainBound := newChainBoundState()

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	require.Empty(t, events, "account target with no witness on unanchored archived ledger stays permissive")
}

// TestVerifySkippedOrder_MetadataNotFoundAccountWitnessedAbsenceStaysPermissive
// pins the legitimate account skip: a live Set then a live Delete before seq
// means the key was genuinely absent, so the NOT_FOUND skip is accepted (the
// `present` guard sees present=false with a witness, and no escape is needed).
func TestVerifySkippedOrder_MetadataNotFoundAccountWitnessedAbsenceStaysPermissive(t *testing.T) {
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

	// Set at 3, Delete at 5 → absent at 7 on an anchored, non-archived ledger.
	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}
	chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{
		"alice": {"role": {{seq: 3, exists: true}, {seq: 5, exists: false}}},
	}

	payload := skippedPayloadWithContext(reason, map[string]string{
		"target": "alice",
		"key":    "role",
	})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, false, false, false)
	require.Empty(t, events, "live delete witness proves absence → legitimate account skip accepted")
}

// TestVerifySkippedOrder_MetadataNotFoundTxTargetInconclusiveStaysPermissive
// confirms the tx-id target still stays permissive on an unanchored, archived
// ledger with an empty timeline (the escape is now kind-agnostic).
func TestVerifySkippedOrder_MetadataNotFoundTxTargetInconclusiveStaysPermissive(t *testing.T) {
	t.Parallel()

	reason := commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	expected := map[uint64]*expectedSkippableOrder{
		7: {
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "123", // genuine transaction id
			metadataKey:    "role",
		},
	}

	// Unanchored ledger (no live CreateLedger), archived chapters exist,
	// empty timeline → inconclusive → permissive.
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
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "123", // genuine transaction id
			metadataKey:    "role",
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
			reasons:        []commonpb.ErrorReason{reason},
			ledger:         "L",
			metadataTarget: "123",
			metadataKey:    "role",
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
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
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
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
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
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
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
			reasons:            []commonpb.ErrorReason{reason},
			ledger:             "L",
			accountTypeName:    "customer",
			isAccountTypeOrder: true,
		},
	}

	chainBound := newChainBoundState()
	chainBound.ledgerCreationSeenLive["L"] = struct{}{}

	payload := skippedPayloadWithContext(reason, map[string]string{"name": "customer"})

	events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, true, false, false)
	requireInvalidSkipEvent(t, events, 7)
}

// TestVerifySkippedOrder_WitnessBasedReasonMatrix is the consolidated matrix
// for the witness-based skip reasons (METADATA_NOT_FOUND on account- and
// tx-id targets, ACCOUNT_TYPE_ALREADY_EXISTS/NOT_FOUND). It exercises every
// (reason × target × anchoring × presence-source) combination the shared
// archiveInconclusive helper governs and pins the expected verdict:
//
//   - a live/baseline WITNESS of the disqualifying state → forged skip caught
//     (INVALID_SKIP), regardless of anchoring/archive;
//   - genuinely inconclusive (no witness, unanchored, archived, no baseline)
//     → permissive;
//   - live-created ledger OR baseline-folded → empty timeline is authoritative
//     → the skip is verified normally (permissive when it agrees with the
//     chain, caught when it contradicts).
//
// CONFLICT / ALREADY_REVERTED are intentionally excluded (claim/first-seen
// structure, no per-key witness) — see their dedicated tests.
func TestVerifySkippedOrder_WitnessBasedReasonMatrix(t *testing.T) {
	t.Parallel()

	const (
		mdNotFound = commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
		atExists   = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
		atNotFound = commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND
	)

	// presence models the (ledger, target/name) timeline state fed to the
	// verifier: none = empty timeline, present = a live/baseline Set/Add
	// before seq, absent = a live/baseline Delete/Remove before seq.
	type presence int
	const (
		none presence = iota
		present
		absent
	)

	// anchoring of the ledger.
	type anchor int
	const (
		unanchored     anchor = iota // no CreateLedger live, no baseline
		liveCreated                  // CreateLedger seen in the live range
		baselineFolded               // baselineChainStateAvailable, ledger folded
	)

	cases := []struct {
		name       string
		reason     commonpb.ErrorReason
		target     string // metadata target (account addr or tx id); "" for account-type
		acctType   string // account-type name; "" for metadata
		pres       presence
		anchor     anchor
		archived   bool
		wantReject bool
	}{
		// METADATA_NOT_FOUND — account target.
		{"md acct present witnessed → reject", mdNotFound, "alice", "", present, unanchored, true, true},
		{"md acct absent witnessed → accept", mdNotFound, "alice", "", absent, unanchored, true, false},
		{"md acct empty unanchored+archived → inconclusive permissive", mdNotFound, "alice", "", none, unanchored, true, false},
		{"md acct empty no-archive → proven absent accept", mdNotFound, "alice", "", none, unanchored, false, false},
		{"md acct empty live-created+archived → proven absent accept", mdNotFound, "alice", "", none, liveCreated, true, false},
		// METADATA_NOT_FOUND — tx-id target (numeric).
		{"md tx present witnessed → reject", mdNotFound, "123", "", present, unanchored, true, true},
		{"md tx empty unanchored+archived → inconclusive permissive", mdNotFound, "123", "", none, unanchored, true, false},
		{"md tx empty live-created+archived → proven absent accept", mdNotFound, "123", "", none, liveCreated, true, false},
		{"md tx empty baseline-folded+archived → proven absent accept", mdNotFound, "123", "", none, baselineFolded, true, false},
		// ACCOUNT_TYPE_NOT_FOUND — expects ABSENT.
		{"at notfound present witnessed → reject", atNotFound, "", "customer", present, unanchored, true, true},
		{"at notfound empty unanchored+archived → inconclusive permissive", atNotFound, "", "customer", none, unanchored, true, false},
		{"at notfound empty live-created+archived → proven absent accept", atNotFound, "", "customer", none, liveCreated, true, false},
		{"at notfound empty baseline-folded+archived → proven absent accept", atNotFound, "", "customer", none, baselineFolded, true, false},
		// ACCOUNT_TYPE_ALREADY_EXISTS — expects PRESENT.
		{"at exists present witnessed → accept", atExists, "", "customer", present, unanchored, true, false},
		{"at exists empty unanchored+archived → inconclusive permissive", atExists, "", "customer", none, unanchored, true, false},
		{"at exists empty live-created+archived → proven absent reject", atExists, "", "customer", none, liveCreated, true, true},
		{"at exists empty baseline-folded+archived → proven absent reject", atExists, "", "customer", none, baselineFolded, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			isMetadata := tc.acctType == ""
			chainBound := newChainBoundState()
			if tc.anchor == liveCreated {
				chainBound.ledgerCreationSeenLive["L"] = struct{}{}
			}

			// Seed the timeline for the presence source. Both live and
			// baseline seeds are appended at seq < 7; baselineChainStateAvailable
			// is toggled separately below to model whether the fold ran.
			seedSeq := uint64(3)
			mut := func(exists bool) []chainBoundMutation {
				return []chainBoundMutation{{seq: seedSeq, exists: exists}}
			}
			switch tc.pres {
			case present:
				if isMetadata {
					chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{tc.target: {"role": mut(true)}}
				} else {
					chainBound.accountTypes["L"] = map[string][]chainBoundMutation{tc.acctType: mut(true)}
				}
			case absent:
				if isMetadata {
					chainBound.metadata["L"] = map[string]map[string][]chainBoundMutation{tc.target: {"role": mut(false)}}
				} else {
					chainBound.accountTypes["L"] = map[string][]chainBoundMutation{tc.acctType: mut(false)}
				}
			case none:
				// empty timeline
			}

			baselineChainState := tc.anchor == baselineFolded
			// A baseline-folded ledger is also anchored via ledgerCreationSeen
			// (foldBaselineLedgers), but archiveInconclusive keys on the
			// baseline flag + ledgerCreationSeenLive, so no extra seeding needed.

			var (
				expected map[uint64]*expectedSkippableOrder
				payload  *commonpb.LedgerLogPayload
			)
			if isMetadata {
				expected = map[uint64]*expectedSkippableOrder{
					7: {reasons: []commonpb.ErrorReason{tc.reason}, ledger: "L", metadataTarget: tc.target, metadataKey: "role"},
				}
				payload = skippedPayloadWithContext(tc.reason, map[string]string{"target": tc.target, "key": "role"})
			} else {
				expected = map[uint64]*expectedSkippableOrder{
					7: {reasons: []commonpb.ErrorReason{tc.reason}, ledger: "L", accountTypeName: tc.acctType, isAccountTypeOrder: true},
				}
				payload = skippedPayloadWithContext(tc.reason, map[string]string{"name": tc.acctType})
			}

			events := captureEventsArchive(t, "L", 7, payload, expected, chainBound, tc.archived, false, baselineChainState)
			if tc.wantReject {
				requireInvalidSkipEvent(t, events, 7)
			} else {
				require.Empty(t, events, "expected permissive/accepted verdict")
			}
		})
	}
}
