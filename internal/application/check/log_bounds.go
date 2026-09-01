package check

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// readStoredLogMax returns the highest log sequence the store holds, taken from
// the KEY, or 0 when the store holds no log row.
//
// Check()'s replay loop accumulates the same value as it walks, and that is the
// value the bound normally uses. This is for the paths that skip the replay loop
// and would otherwise pass a zero maximum to compareLogBounds, which reads as
// "the store holds no log" and silently compares nothing.
//
// The iterator bounds MUST stay identical to the replay loop's, or the two
// callers would disagree about which rows exist. A reverse seek is enough: the
// key encoding is big-endian, so the last key in the range is the maximum.
func readStoredLogMax(reader dal.PebbleReader) (uint64, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneCold, dal.SubColdLog},
		UpperBound: []byte{dal.ZoneCold, dal.SubColdLog, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	})
	if err != nil {
		return 0, fmt.Errorf("creating log iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return 0, iter.Error()
	}

	seq, err := decodeLogSequence(iter.Key())
	if err != nil {
		return 0, fmt.Errorf("reading the highest stored log key: %w", err)
	}

	return seq, iter.Error()
}

// logKeyLen is the exact byte length of a log row key:
// [ZoneCold(1)][SubColdLog(1)][sequence(8)].
const logKeyLen = 10

// decodeLogSequence reads the sequence a log row key encodes, rejecting any key
// that is not exactly logKeyLen bytes.
//
// The length check is not defensive noise. Both log iterators are bounded below
// by the bare two-byte zone prefix and above by a ten-byte all-0xFF key, so ANY
// key of length 2..9 under that prefix is inside the range: a short key filled
// toward 0xFF is a strict prefix of the upper bound, and "shorter is less" sorts
// it ABOVE every ten-byte key whose first sequence byte is below 0xFF — that is,
// above every realistic sequence, which is exactly where readStoredLogMax's
// reverse seek lands.
//
// Slicing such a key unchecked is bounds-checked by Go against CAPACITY rather
// than length, so the outcome depends on Pebble's internal key buffer: either a
// panic, or eight adjacent buffer bytes decoded as a fabricated sequence that
// the replay loop then treats as real. The fabricated value is the worse half —
// in the replay loop it feeds the `for expectedSeq < seq` emission below, which
// on a near-2^64 sequence never terminates.
//
// Every repository writer goes through KeyBuilder.PutUint64 and emits exactly
// ten bytes, so a short key is Pebble-level corruption or direct store access.
// That input is inside this component's threat model all the same:
// ValidateRestore runs Check() over an untrusted foreign staged backup.
//
// Reported as an error rather than as a finding event: the row's claimed
// sequence is unknowable, so the pass can neither replay it nor bound it, and
// continuing would mean guessing.
func decodeLogSequence(key []byte) (uint64, error) {
	if len(key) != logKeyLen {
		return 0, fmt.Errorf("log key %x is %d bytes, want %d ([ZoneCold][SubColdLog][sequence(8)])",
			key, len(key), logKeyLen)
	}

	return binary.BigEndian.Uint64(key[2:]), nil
}

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
// store does not hold — is a missing log like any other, and it is already
// covered, but by two different passes depending on where the hole is:
//
//   - INTERIOR holes go to the gap detection in the replay loop. That loop runs
//     inside the iteration (`for expectedSeq < seq`), so it fills the span
//     between two stored rows and stops at the highest stored key; expectedSeq is
//     never consulted after the loop. SEQUENCE_GAP, one per missing sequence.
//   - A lost TAIL above the highest stored key is invisible to that loop, and to
//     this pass. It is reported by compareBoundaries: the stored Boundary row
//     keeps its NextLogId while the replayed expectation shrinks with the deleted
//     logs, so it comes out as BOUNDARY_MISMATCH.
//
// Adding a third finding for either would double-report one fault under two
// names.
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
// store with archived chapters, log rows retained at or under the archive
// boundary, and ZERO live audit entries would report a false LOG_UNAUDITED. (The
// baseline checkpoint is irrelevant to it — this pass also runs on the
// baseline-less archived path, where storedLogMax comes from readStoredLogMax.)
// That shape is unreachable through the archive flow — archiving
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
