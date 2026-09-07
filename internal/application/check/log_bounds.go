package check

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// logBoundsVerifier re-derives the highest log sequence the audit chain
// accounts for, and compares it to the highest log sequence the store actually
// holds.
//
// It closes the one truncation class no other pass can see. The interior gap
// scan in Check()'s log loop reports a sequence missing BETWEEN two surviving
// rows, but it has no oracle for a deleted TAIL: drop the top N Log rows and
// every projection rebuilt from the survivors stays self-consistent, so the
// whole run reports nothing. That is exactly the restore failure mode this pass
// exists to catch. The reverse direction — a Log row above everything the audit
// produced — is the injected-row class: Log rows are not hash-bound, so the
// audit success ranges are the only statement of which positions the FSM ever
// allocated.
//
// PREMISE. Over a chain-verified audit range, the union of
// [AuditSuccess.min, AuditSuccess.max] over the successes with max > 0 is ONE
// contiguous interval starting at 1, and its upper end is the expected highest
// stored log key. The producer side makes this hold:
//
//   - Log sequences have exactly two producing call sites, both in
//     RequestProcessor.ProcessOrders (internal/domain/processing/processor.go,
//     the skip-log and normal-log paths). Both allocate through
//     Scope.IncrementNextSequenceID, which is a plain +1, and both immediately
//     fold the allocated id into OrdersResult.Min/MaxLogSequence — so no
//     allocation escapes the range it is reported in.
//   - A no-log outcome (an idempotent mirror replay) returns before allocating,
//     so it consumes no id.
//   - A failed proposal never reaches WriteSet.Merge, which is the only place
//     FSMState.NextSequenceID is advanced (internal/infra/state/write_set.go);
//     its ticked ids are discarded and the next proposal reuses them. The
//     failure entry it writes carries no success range at all.
//   - An idempotent proposal replay returns the recorded outcome without
//     running the pipeline: no new log, no audit entry
//     (internal/infra/state/machine.go).
//   - The per-order overlay used by skippable orders never allocates a log
//     sequence (ProcessOrders allocates on the parent scope), so a dropped
//     overlay leaks no id.
//
// A violation of the premise is therefore unreachable by contract, and this
// type treats it as an invariant failure rather than deriving a bound from it
// (invariant #7).
//
// ONLY THE UPPER END IS COMPARED HERE. The interval the premise describes is
// [1, expectedMax], and compare() below checks the stored head against
// expectedMax alone: sequence 0 sits below every audited range, so a Log row
// there is invisible to this comparison — on a store whose real head is N the
// row moves neither side, and on a store where it is the only row the stored
// and expected heads are both 0. The interior gap scan in Check()'s log loop
// cannot see it either: that scan is seeded at expectedSeq 1 and never looks
// beneath it. The lower end is therefore pinned in the log loop, where the key
// is decoded — a row at key sequence 0 is reported as LOG_UNAUDITED there, off
// the key and before this fold runs, because FSMState.NextSequenceID is seeded
// at 1 and observeSuccess pins the first audited range's minimum at 1.
//
// RECONCILIATION with replay.ProposalBoundaryTracker
// (internal/domain/replay/replay.go), whose comment states that audit ranges
// "may include idempotent references to older logs". That reads as a
// counter-example to the premise, and it is not one. AuditSuccess.{min, max}
// are computed over FRESHLY CREATED logs only; what could point back at an
// older log was a legacy pre-f9ee1e829 per-order REFERENCE item, never the
// entry's success range — see the signing-fold comment in checker.go, which
// filters those items out through this very [min, max] window. v3 is
// unreleased and carries no compatibility burden with pre-release formats
// (AGENTS.md, "Release status"), so no store this pass runs against holds such
// an entry. The tracker's tolerance is legacy defensiveness on the ITEM side,
// not a statement that a success range can reach backwards.
//
// The expectation is never seeded from the log stream under test — that is the
// same rule signingVerifier and clusterPolicyVerifier follow, and here it is
// what makes a missing tail visible at all.
type logBoundsVerifier struct {
	// expectedMax is the highest log sequence the chain-verified success ranges
	// account for; 0 when no success range carried a log.
	expectedMax uint64
	// incompleteReason is the message to report instead of a comparison, empty
	// while the derivation is sound. First cause wins: it is set either by a
	// chain break (markLiveTruncated) or by an inverted or discontinuous
	// success range (observeSuccess), and once set the fold stops advancing
	// expectedMax.
	//
	// Fail-closed suppression, for the same reason signingVerifier suppresses
	// its key comparison on a truncated fold: a bound derived from a PREFIX of
	// the history is unsound in both directions. Every surviving log above the
	// break reads as unaudited, and — on a store whose tail really is missing —
	// the gap the partial bound reports is arbitrary. Reporting that the bound
	// could not be derived is the honest outcome.
	incompleteReason string
}

func newLogBoundsVerifier() *logBoundsVerifier {
	return &logBoundsVerifier{}
}

// markLiveTruncated records that the live audit fold stopped short of the end of
// the range. Called from every non-error early exit in verifyAuditHashChain,
// alongside the signing and cluster-policy verifiers.
func (v *logBoundsVerifier) markLiveTruncated() {
	v.markIncomplete("the expected log range could not be derived over the whole history: the audit range " +
		"was cut short by a hash chain break, so every log produced after it is unaccounted for. The " +
		"stored log bound is not compared for this run rather than reported against a partial bound")
}

// markIncomplete records the first reason the derivation cannot be trusted.
// First cause wins: a later symptom of the same break would only bury the
// finding that explains it.
func (v *logBoundsVerifier) markIncomplete(reason string) {
	if v.incompleteReason != "" {
		return
	}

	v.incompleteReason = reason
}

// observeSuccess folds one chain-verified SUCCESS audit entry into the expected
// bound. Callers MUST only pass entries whose hash has just been verified: an
// unverified range carries no statement about which logs the FSM produced.
//
// Entries with min == max == 0 carry no freshly created log (an all-idempotent
// replay, or a proposal whose orders produced no log) and are ignored — they
// neither advance nor interrupt the interval. An entry with min > 0 and max ==
// 0 is NOT that shape: it is an inverted range, and it is reported as one.
//
// The three range shapes are therefore tested in a fixed order — inversion,
// then log-less, then discontinuity — because the log-less short-circuit would
// otherwise absorb every inverted range whose max happens to be 0.
func (v *logBoundsVerifier) observeSuccess(entry *auditpb.AuditEntry) {
	if v.incompleteReason != "" {
		return
	}

	success := entry.GetSuccess()

	minSeq, maxSeq := success.GetMinLogSequence(), success.GetMaxLogSequence()

	// The ranges are hash-bound and the producer allocates contiguously from a
	// counter no other path advances, so a hole or an inversion here is
	// unreachable by contract. Invariant #7 forbids swallowing it, and the
	// checker's contract is to report and continue rather than fail the run, so
	// it surfaces as a loud finding and suppresses every bound derived from the
	// premise that just failed. Check() therefore still returns nil, and the
	// remaining passes still run.
	//
	// Inversion is checked FIRST, before the log-less return below. min > 0
	// with max == 0 is an inverted range that the log-less test would classify
	// as "no log created" and drop in silence, which is the opposite of what
	// this file's header and the checker documentation promise.
	if minSeq > maxSeq {
		v.markIncomplete(fmt.Sprintf(
			"invariant: audit entry %d declares an inverted log range %d..%d, with the minimum log "+
				"sequence above the maximum; a success range is either empty (0..0) or ascending, so the "+
				"expected log bound cannot be derived and the stored bound is not compared",
			entry.GetSequence(), minSeq, maxSeq))

		return
	}

	// minSeq <= maxSeq holds here, so maxSeq == 0 implies minSeq == 0: the
	// genuinely log-less success.
	if maxSeq == 0 {
		return
	}

	if minSeq != v.expectedMax+1 {
		v.markIncomplete(fmt.Sprintf(
			"invariant: audit entry %d declares log range %d..%d, but the chain-verified success ranges "+
				"before it end at %d; the audited log ranges must be contiguous from sequence 1, so the "+
				"expected log bound cannot be derived and the stored bound is not compared",
			entry.GetSequence(), minSeq, maxSeq, v.expectedMax))

		return
	}

	v.expectedMax = maxSeq
}

// compare reports how the highest stored log KEY sequence relates to the
// audit-derived bound. It bounds the audited interval from ABOVE only; its
// lower end is pinned in Check()'s log loop (see the header).
//
// storedMax must be read off the Pebble KEY of the last Log row, never off the
// value's `sequence` field (query.ReadLastSequence): that field is not
// hash-bound and editing it is itself a reported finding
// (CHECK_STORE_ERROR_TYPE_LOG_SEQUENCE_MISMATCH), so a bound taken from it
// could be moved by the same edit it is meant to catch.
//
// Both divergences are reported as ONE aggregated event with a row count rather
// than one event per sequence: a truncated or injected tail can be millions of
// rows, and the interior per-sequence gap scan already covers the small-hole
// case it is useful for.
func (v *logBoundsVerifier) compare(storedMax uint64, callback func(*servicepb.CheckStoreEvent)) {
	if v.incompleteReason != "" {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE,
			v.incompleteReason, 0, "", "", ""))

		return
	}

	switch {
	case storedMax < v.expectedMax:
		// Deleted tail. The audit chain proves those logs existed, and no
		// surviving row is adjacent to the hole, so the interior scan in the log
		// loop cannot see it.
		first := storedMax + 1

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
			fmt.Sprintf("log sequences %d..%d are missing: the audit chain accounts for logs up to %d "+
				"but the highest stored log is %d (%d logs)",
				first, v.expectedMax, v.expectedMax, storedMax, v.expectedMax-storedMax),
			first, "", "", ""))
	case storedMax > v.expectedMax:
		first := v.expectedMax + 1

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED,
			fmt.Sprintf("log sequences %d..%d have no audited origin: the audit chain accounts for logs "+
				"up to %d but the store holds logs up to %d (%d logs)",
				first, storedMax, v.expectedMax, storedMax, storedMax-v.expectedMax),
			first, "", "", ""))
	}
}
