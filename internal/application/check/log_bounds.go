package check

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// compareLogBounds bounds the log stream from above with the audit hash chain.
//
// The audit chain is the only hash-bound dataset in the store and therefore the
// only business truth (invariant #8). Log rows are not hash-bound: they are a
// projection the checker REPLAYS, so a row appended past the last audited
// proposal is not merely unverified, it is authoritative for every pass that
// consumes the replay. Its transactions, volumes and metadata all become part of
// the expectation the projections are then compared against, and the store comes
// back clean. Nothing else in Check() contradicts such a row — which is what
// this pass exists to do.
//
// The comparison is deliberately max-only:
//
//   - storedLogMax is the highest sequence the log KEYS hold, captured in the
//     replay loop. The key is what AppendLogs derives from the FSM counter; the
//     value's `sequence` field is not hash-bound (the EN-1526 bypass, now
//     asserted against at its own key/value disagreement check).
//   - chainBound.expectedLogMax is the highest sequence the chain authenticates,
//     accumulated over the chain walk in verifyAuditHashChain.
//
// A per-sequence set-membership comparison was rejected. Every log allocated
// inside the audited range is already covered: the replay loop reports each
// missing sequence as SEQUENCE_GAP, verifySkippedOrder pins each log's outcome
// to its chain-bound order, and the projection passes compare what the logs
// fold into. The maxima are the one relation none of those can see, because
// every one of them is driven FROM the log stream and so cannot notice the
// stream ending later than the chain says it does.
//
// Only one direction is reported here. Stored above audited means a row exists
// that no proposal produced. The opposite — the chain authenticating a log the
// store does not hold — is a missing log like any other and is already reported
// as SEQUENCE_GAP by the gap detection in the replay loop, which walks up to the
// highest stored sequence; adding a second finding for it would double-report
// the same fault under two names.
//
// archiveEndSeq is deliberately absent from this comparison, and must stay
// absent. It is not a weaker guarantee than the chain — it is a DIFFERENT one:
//
//   - the audit chain is a keyed MAC. processing.NewHashGenerator derives its
//     key from the ClusterID (blake3AuditKeyContext + clusterID) and hashes with
//     blake3.NewKeyed, so an attacker without the cluster identity cannot
//     produce a valid entry hash at all.
//   - the chapter sealing hash is UNKEYED. verifySealingHash recomputes it with
//     plain blake3.New() over (id, close_sequence, last_audit_hash, state_hash).
//     close_sequence is covered by it, but whoever edits close_sequence just
//     recomputes the hash and the check passes. That pass catches corruption,
//     not tampering.
//
// So archiveEndSeq is attacker-controlled, which is exactly why it must never
// gate which stored rows are compared, nor raise the threshold they are compared
// against. Clamping to max(expectedLogMax, archiveEndSeq) — the obvious "fix" for
// the false positive below — would let a forged close_sequence lift the bound
// above an injected log row and exclude it from the only check that can see it.
// That is not a refinement of EN-1526, it IS the EN-1526 defect shape: a
// non-hash-bound projection field steering verification, rebuilt in a new place.
//
// The accepted consequence, stated plainly so nobody has to rediscover it: a
// store with archived chapters, a baseline checkpoint, log rows retained at or
// under the archive boundary, and ZERO live audit entries would report a false
// LOG_UNAUDITED. That shape is unreachable through the archive flow — archiving
// emits its own logs above the range it purges, so at least one log and its live
// audit entry always survive (see the note above the signingVerifier
// construction in Check) — and a false positive on an unreachable shape is the
// correct trade against a real forgeable-threshold bypass.
//
// Neither side is compared when the chain walk was truncated: see
// logCoverageIncomplete.
func compareLogBounds(chainBound *chainBoundState, storedLogMax uint64, callback func(*servicepb.CheckStoreEvent)) {
	// A truncated walk leaves expectedLogMax the maximum of a PREFIX of the
	// history, so every log above the break would look unaudited. Report the
	// gap once and compare nothing — the same choice, for the same reason, as
	// SIGNING_VERIFICATION_INCOMPLETE.
	if chainBound.logCoverageIncomplete {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_VERIFICATION_INCOMPLETE,
			"the audit chain walk stopped before the end of the live range, so the audited log maximum is only a prefix maximum: the log stream cannot be bounded in this run",
			0, "", "", ""))

		return
	}

	if storedLogMax > chainBound.expectedLogMax {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED,
			fmt.Sprintf("the store holds log %d but the audit chain authenticates no log above %d: the log stream extends %d sequence(s) past the audited maximum, so it was written outside the audited apply path",
				storedLogMax, chainBound.expectedLogMax, storedLogMax-chainBound.expectedLogMax),
			storedLogMax, "", "", ""))
	}
}
