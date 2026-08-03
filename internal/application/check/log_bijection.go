package check

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// logRange is a contiguous, inclusive log-sequence interval.
type logRange struct {
	min uint64
	max uint64
}

// logRangeSet is a set of log sequences held as sorted, merged, inclusive
// intervals.
//
// Interval representation rather than a per-sequence map is deliberate.
// AuditSuccess.[min,max]_log_sequence is a contiguous inclusive range
// (misc/proto/audit.proto), and log_sequence never advances on a failed
// proposal, so successive success ranges are adjacent: a healthy store
// collapses to a SINGLE interval. Memory is O(number of anomalies) rather
// than O(number of logs), so unlike the replay accumulators in
// replay_store.go this needs no Pebble-backed scratch.
//
// The zero value is ready to use.
type logRangeSet struct {
	ranges     []logRange
	normalized bool
}

// add records [minSeq, maxSeq] as a member interval. A zero bound means the
// range carries no logs (a proposal in which every order was an idempotent
// replay) and contributes nothing; an inverted range is likewise ignored —
// the caller that produced it reports the malformed entry separately.
// Intervals may be added in any order.
func (s *logRangeSet) add(minSeq, maxSeq uint64) {
	if minSeq == 0 || maxSeq == 0 || minSeq > maxSeq {
		return
	}

	// Coalesce eagerly when the incoming range starts inside the previous one or
	// immediately after it. This is what makes the O(anomalies) memory claim
	// above true: every caller feeds ranges in ascending order over the whole
	// history — one per audit entry in verifyAuditStructure, one per stored log
	// in compareLogs — so a plain append would grow the slice to O(history) and
	// hand normalize() a sort of that size, exactly the accumulation the
	// Pebble-backed replay store exists to avoid. normalize() would merge these
	// anyway, so the result is unchanged; only the peak footprint differs.
	//
	// Safe with out-of-order adds too: the guard requires minSeq >= the last
	// range's min, so a range that belongs earlier is appended and left for
	// normalize() to sort.
	if n := len(s.ranges); n > 0 && minSeq >= s.ranges[n-1].min && minSeq <= s.ranges[n-1].max+1 {
		if maxSeq > s.ranges[n-1].max {
			s.ranges[n-1].max = maxSeq
			s.normalized = false
		}

		return
	}

	s.ranges = append(s.ranges, logRange{min: minSeq, max: maxSeq})
	s.normalized = false
}

// normalize sorts the intervals ascending and coalesces overlapping or
// adjacent ones. Idempotent.
func (s *logRangeSet) normalize() {
	if s.normalized {
		return
	}

	sort.Slice(s.ranges, func(i, j int) bool { return s.ranges[i].min < s.ranges[j].min })

	// In-place coalesce: writes only ever land at an index at or below the
	// current read index, so aliasing the backing array is safe.
	merged := s.ranges[:0]

	for _, r := range s.ranges {
		if n := len(merged); n > 0 && r.min <= merged[n-1].max+1 {
			if r.max > merged[n-1].max {
				merged[n-1].max = r.max
			}

			continue
		}

		merged = append(merged, r)
	}

	s.ranges = merged
	s.normalized = true
}

// intervals returns the normalized intervals. The result aliases internal
// state and must not be mutated.
func (s *logRangeSet) intervals() []logRange {
	s.normalize()

	return s.ranges
}

// contains reports whether seq is a member of the set.
func (s *logRangeSet) contains(seq uint64) bool {
	s.normalize()

	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].max >= seq })

	return i < len(s.ranges) && seq >= s.ranges[i].min
}

// total returns the number of distinct sequences in the set.
func (s *logRangeSet) total() uint64 {
	s.normalize()

	var n uint64

	for _, r := range s.ranges {
		n += r.max - r.min + 1
	}

	return n
}

// empty reports whether the set holds no sequences.
func (s *logRangeSet) empty() bool {
	s.normalize()

	return len(s.ranges) == 0
}

// gapsIn returns the sub-ranges of r that the set does NOT cover, ascending.
// An empty result means r is fully covered.
//
// Interval arithmetic, never per-sequence iteration: a 2^62-wide uncovered span
// yields ONE logRange, not 2^62 loop turns. That is what lets compareLogs
// enumerate missing logs without trusting the width of a store-supplied range
// (see maxEnumeratedGapSequences).
//
// O(log(intervals) + overlapping intervals): the binary search skips the
// intervals below r, and after it every remaining interval either overlaps r or
// ends the walk.
func (s *logRangeSet) gapsIn(r logRange) []logRange {
	if r.min > r.max {
		return nil
	}

	s.normalize()

	var gaps []logRange

	cursor := r.min

	// First interval that can possibly reach into r. normalize() leaves the
	// intervals sorted, disjoint and non-adjacent, so from here on every
	// interval starts at or after cursor.
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].max >= r.min })

	for _, iv := range s.ranges[i:] {
		if iv.min > r.max {
			break
		}

		if iv.min > cursor {
			gaps = append(gaps, logRange{min: cursor, max: iv.min - 1})
		}

		// Returning here rather than advancing keeps cursor from overflowing
		// when iv.max is the maximum uint64.
		if iv.max >= r.max {
			return gaps
		}

		cursor = iv.max + 1
	}

	return append(gaps, logRange{min: cursor, max: r.max})
}

// chainVerification bundles what verifyAuditHashChain derives from the
// hash-verified audit range. A struct rather than extra out-parameters: the
// function already takes 7 arguments, and its five direct test call sites
// discard this return with `_`, so widening it later costs no test churn —
// which is the point.
type chainVerification struct {
	// skippable is the per-log-sequence skippable_reasons whitelist consumed
	// by verifySkippedOrder during log replay.
	skippable map[uint64]*expectedSkippableOrder
	// authenticated holds the log sequences the chain proves must exist in
	// the store. Fed by every verified AuditSuccess range.
	authenticated logRangeSet
	// unverifiable holds log spans the checker can prove nothing about,
	// because the entry covering them is structurally malformed. compareLogs
	// skips these rather than reporting false gaps across them.
	unverifiable logRangeSet
	// sequences is the set of audit sequences whose AuditEntry was read and
	// hash-verified in this run. scanOrphanAuditItems uses it to find
	// AuditItem groups that have no entry.
	sequences map[uint64]struct{}
	// archiveEndAuditSeq is the highest archived audit sequence. Orphan
	// AuditItem rows at or below it are legitimate: executePurge deletes
	// SubColdAudit and SubColdLog for an archived chapter but deliberately
	// leaves SubColdAuditItem in place.
	archiveEndAuditSeq uint64
}

// newChainVerification returns a chainVerification with its maps ready.
func newChainVerification() *chainVerification {
	return &chainVerification{
		skippable: make(map[uint64]*expectedSkippableOrder),
		sequences: make(map[uint64]struct{}),
	}
}

// unverifiableEvent builds a CheckStoreUnverifiableRange event. It is NOT an
// error: it must never affect the CLI exit code or the JSON `valid` flag. It
// exists so that a range the checker could not authenticate is declared rather
// than silently passed over — the absence of a finding in an unauthenticated
// range is not proof.
//
// Every occurrence marks a defect; a healthy cluster, archived or not, emits
// none. reason is typed so operators route on it rather than string-matching
// message, since the two causes need different runbooks.
//
// rangeStart/rangeEnd are inclusive. Log sequences are 1-based, so pass 0/0
// when the bounds genuinely could not be determined — consumers render that as
// unknown, never as a literal "0-0".
func unverifiableEvent(
	reason servicepb.CheckStoreUnverifiableReason,
	message string,
	rangeStart, rangeEnd uint64,
) *servicepb.CheckStoreEvent {
	return &servicepb.CheckStoreEvent{
		Type: &servicepb.CheckStoreEvent_UnverifiableRange{
			UnverifiableRange: &servicepb.CheckStoreUnverifiableRange{
				Reason:     reason,
				Message:    message,
				RangeStart: rangeStart,
				RangeEnd:   rangeEnd,
			},
		},
	}
}

// verifyAuditStructure checks a hash-verified AuditEntry for internal
// consistency and records what it authenticates.
//
// None of these checks detect tampering: order_count, order_index and
// log_sequence are all hash-bound (state.BuildHashedHeaderPayload,
// state.BuildPerItemPayload), and query.ReadAuditItems folds EVERY row in the
// entry's prefix into the hash, so mutating, deleting or injecting an item
// already breaks the chain. They exist because compareLogs CONSUMES these
// fields: an internally inconsistent entry would yield a wrong authenticated
// set and therefore false missing/extra findings. Per invariant #8 the audit is
// the sole authority, so a self-contradictory entry must surface.
//
// A violating entry contributes its span to v.unverifiable instead of
// v.authenticated, so compareLogs proves nothing across it.
//
// Returns false when the entry was rejected.
func verifyAuditStructure(
	entry *auditpb.AuditEntry,
	items []*auditpb.AuditItem,
	v *chainVerification,
	callback func(*servicepb.CheckStoreEvent),
) bool {
	report := func(format string, args ...any) {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_AUDIT_STRUCTURE_INVALID,
			fmt.Sprintf("audit entry %d: "+format, append([]any{entry.GetSequence()}, args...)...),
			logSequenceFromAuditEntry(entry), "", "", "",
		))
	}

	success := entry.GetSuccess()

	// blank marks the entry's log span unprovable AND declares it, so
	// compareLogs skipping that span is visible rather than silent. Only a
	// success entry has a span; a failure produced no logs, so there is nothing
	// to blank.
	blank := func() {
		if success == nil {
			return
		}

		minLog, maxLog := success.GetMinLogSequence(), success.GetMaxLogSequence()

		v.unverifiable.add(minLog, maxLog)
		callback(unverifiableEvent(
			servicepb.CheckStoreUnverifiableReason_CHECK_STORE_UNVERIFIABLE_REASON_MALFORMED_AUDIT_ENTRY,
			fmt.Sprintf("audit entry %d is structurally malformed; its log span cannot be proven either way",
				entry.GetSequence()),
			minLog, maxLog,
		))
	}

	// Item count and index layout are checked for BOTH outcomes.
	// buildAuditItems (internal/infra/state/audit.go:57) is called from the
	// closure shared by success and failure (machine.go:1435) and is sized on
	// the full serializedOrders set, so a failure carries one item per
	// submitted order exactly like a success — each with LogSequence 0 because
	// the failure path passes no logs.
	if uint32(len(items)) != entry.GetOrderCount() {
		report("order_count is %d but %d audit items are stored", entry.GetOrderCount(), len(items))
		blank()

		return false
	}

	seenIndex := make(map[uint32]struct{}, len(items))

	for _, item := range items {
		idx := item.GetOrderIndex()

		if idx >= entry.GetOrderCount() {
			report("audit item order_index %d is outside [0,%d)", idx, entry.GetOrderCount())
			blank()

			return false
		}

		if _, dup := seenIndex[idx]; dup {
			report("audit item order_index %d is duplicated", idx)
			blank()

			return false
		}

		seenIndex[idx] = struct{}{}
	}

	if success == nil {
		// FAILURE: the proposal produced no logs, so no item may claim one.
		// This is the failure-side counterpart to the success range checks and
		// is a genuine contradiction if violated.
		for _, item := range items {
			if seq := item.GetLogSequence(); seq != 0 {
				report("failed proposal item %d claims log sequence %d; a failure produces no logs",
					item.GetOrderIndex(), seq)

				return false
			}
		}

		return true
	}

	minLog, maxLog := success.GetMinLogSequence(), success.GetMaxLogSequence()

	if minLog > maxLog {
		report("success range [%d,%d] is inverted", minLog, maxLog)

		return false
	}

	seenLogSeq := make(map[uint64]struct{}, len(items))

	for _, item := range items {
		logSeq := item.GetLogSequence()
		if logSeq == 0 {
			// An idempotent in-batch reference produced no log of its own.
			continue
		}

		// Legacy pre-f9ee1e829 stores can carry per-order replay REFERENCE
		// items whose log_sequence points at an EARLIER entry's log. Those sit
		// outside [minLog,maxLog] by construction and are filtered rather than
		// flagged, exactly as collectExpectedSkippable does.
		if logSeq < minLog || logSeq > maxLog {
			continue
		}

		if _, dup := seenLogSeq[logSeq]; dup {
			report("log sequence %d is claimed by two audit items", logSeq)
			blank()

			return false
		}

		seenLogSeq[logSeq] = struct{}{}
	}

	v.authenticated.add(minLog, maxLog)

	return true
}

// scanOrphanAuditItems reports AuditItem groups whose audit sequence has no
// AuditEntry. The chain walk iterates ENTRIES, so an injected item group is
// never read and never hashed — the one structural gap the hash chain does not
// cover.
//
// Bounded to audit sequences strictly above archiveEndAuditSeq: executePurge
// deliberately leaves archived AuditItem rows in Pebble (it purges SubColdAudit
// and SubColdLog but not SubColdAuditItem), so orphans below the boundary are
// legitimate and must not be flagged. Compare collectAuditOrderBoundaryEffects,
// which filters for the same reason.
//
// One event per orphaned sequence, not per row.
func (c *Checker) scanOrphanAuditItems(
	reader dal.PebbleReader,
	v *chainVerification,
	callback func(*servicepb.CheckStoreEvent),
) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneCold, dal.SubColdAuditItem},
		UpperBound: []byte{dal.ZoneCold, dal.SubColdAuditItem + 1},
	})
	if err != nil {
		return fmt.Errorf("creating audit item iter for orphan scan: %w", err)
	}

	defer func() { _ = iter.Close() }()

	reported := make(map[uint64]struct{})

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		// Key: [ZoneCold(1)][SubColdAuditItem(1)][audit_seq BE 8][order_idx BE 4]
		if len(key) < 10 {
			return fmt.Errorf("invariant: audit item key %x is shorter than the 10-byte prefix", key)
		}

		auditSeq := binary.BigEndian.Uint64(key[2:10])

		if auditSeq <= v.archiveEndAuditSeq {
			continue
		}

		if _, ok := v.sequences[auditSeq]; ok {
			continue
		}

		if _, done := reported[auditSeq]; done {
			continue
		}

		reported[auditSeq] = struct{}{}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_AUDIT_STRUCTURE_INVALID,
			fmt.Sprintf("audit items stored at sequence %d but no audit entry exists there", auditSeq),
			0, "", "", "",
		))
	}

	return iter.Error()
}

// buildPurgedLogSet returns the log sequences that archiving has removed from
// Pebble: the union of [start_sequence, close_sequence] over every ARCHIVED
// chapter. It also declares chapters whose bounds are unusable.
//
// Per-chapter ranges rather than a single max(close_sequence) floor: if
// chapter 3 reaches ARCHIVED while chapter 2 is still ARCHIVING, that max
// would wrongly treat chapter 2's still-live logs as purged.
//
// The purge is atomic with the status transition — processConfirmArchiveChapter
// sets ARCHIVED and Absorb registers the purge range, and executePurge's
// DeleteRange lands in the same Pebble batch — so no legitimate Log row can
// survive inside a purged range.
//
// This set REFINES which error compareLogs reports; it never suppresses one. A
// stored log is wrong whether it sits in a purged range (LOG_PURGE_RESIDUE, the
// purge did not run) or outside every one (LOG_UNAUDITED, injection). So a
// chapter with unusable bounds cannot produce a false positive — it only costs
// the ability to name the more specific cause, and the log is then reported as
// LOG_UNAUDITED, the conservative claim. That is why nothing is added to an
// unverifiable set here: when the bounds are corrupt the covered range is
// unknown by definition, so there is no range to record. The event is the
// signal; suppression would be both impossible and wrong.
func buildPurgedLogSet(
	chapters []*commonpb.Chapter,
	callback func(*servicepb.CheckStoreEvent),
) *logRangeSet {
	purged := &logRangeSet{}

	for _, ch := range chapters {
		if ch.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			continue
		}

		start, closeSeq := ch.GetStartSequence(), ch.GetCloseSequence()

		if start == 0 || closeSeq == 0 || start > closeSeq {
			callback(unverifiableEvent(
				servicepb.CheckStoreUnverifiableReason_CHECK_STORE_UNVERIFIABLE_REASON_ARCHIVED_CHAPTER_BOUNDS_UNUSABLE,
				fmt.Sprintf("archived chapter %d has unusable log bounds [%d,%d]; logs cannot be attributed to its purged range",
					ch.GetId(), start, closeSeq),
				0, 0,
			))

			continue
		}

		purged.add(start, closeSeq)
	}

	return purged
}

// compareLogs verifies the persisted SubColdLog key set against the log
// sequences the audit hash chain authenticated, in BOTH directions. It is the
// pass that makes Log a checked projection under invariant #8: the rows are
// not hash-bound, and before EN-1526 the only log-set check compared the log
// stream against itself.
//
// One ascending pass over the SubColdLog iterator, then a reverse pass that
// SUBTRACTS the accounted-for sequences (stored plus unverifiable) from each
// authenticated interval by interval arithmetic — never by walking the interval
// sequence by sequence, since its upper edge is a store-supplied uint64 that a
// Pebble-level adversary can inflate (see maxEnumeratedGapSequences). Each
// membership test and each subtraction is a binary search over an interval list
// that a healthy store collapses to a single entry. Time is
// O((logs + intervals) * log(intervals)) plus at most
// maxEnumeratedGapSequences + 1 emissions per missing range, memory
// O(intervals).
//
//   - authenticated, absent from the store -> SEQUENCE_GAP
//   - stored, inside an ARCHIVED chapter's purged range -> LOG_PURGE_RESIDUE
//   - stored, unauthenticated, outside every purged range -> LOG_UNAUDITED
//     (this also catches audit-TAIL truncation: deleting trailing AuditEntry
//     rows orphans the logs they produced, which surface here)
//
// PRECEDENCE: the purged-range check runs FIRST and wins. The two conditions
// are not mutually exclusive — archiving purges a chapter's AuditEntry rows and
// its Log rows in the same batch, so a row surviving in a purged range is
// necessarily also unauthenticated (its audit entry is gone by design, not by
// tampering). Reporting LOG_UNAUDITED there would be technically true but
// misdiagnose a purge-lifecycle failure as an injection. LOG_PURGE_RESIDUE is
// the more specific claim, so it takes precedence; exactly one event is emitted
// per stored sequence.
//
// Sequences inside an unverifiable span are skipped in both directions.
//
// There is deliberately no duplicate category (SubColdLog is keyed by
// sequence, so two rows at one sequence are physically impossible) and no
// failure-associated category (a failed proposal allocates no log sequence at
// all, so such a row is indistinguishable from any other unaudited row).
// Detecting a forged PAYLOAD at an otherwise valid sequence is EN-1531.
func (c *Checker) compareLogs(
	reader dal.PebbleReader,
	authenticated *logRangeSet,
	purged *logRangeSet,
	unverifiable *logRangeSet,
	callback func(*servicepb.CheckStoreEvent),
) error {
	stored := &logRangeSet{}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneCold, dal.SubColdLog},
		UpperBound: []byte{dal.ZoneCold, dal.SubColdLog, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	})
	if err != nil {
		return fmt.Errorf("creating log iterator for bijection: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		if len(key) < 10 {
			return fmt.Errorf("invariant: log key %x is shorter than the 10-byte prefix", key)
		}

		seq := binary.BigEndian.Uint64(key[2:10])
		stored.add(seq, seq)

		if unverifiable.contains(seq) {
			continue
		}

		if authenticated.contains(seq) {
			continue
		}

		if purged.contains(seq) {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_PURGE_RESIDUE,
				fmt.Sprintf("log %d lies inside an archived chapter's purged range; archiving removes it atomically, so no legitimate row can be here", seq),
				seq, "", "", "",
			))

			continue
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_UNAUDITED,
			fmt.Sprintf("log %d is stored but no audit entry authenticates it", seq),
			seq, "", "", "",
		))
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("log iterator error during bijection: %w", err)
	}

	// Reverse direction: authenticated but absent. Derived as a range
	// DIFFERENCE against everything the checker accounts for — the stored rows
	// plus the spans it can prove nothing about — so an authenticated interval
	// is never walked sequence by sequence.
	covered := &logRangeSet{}

	for _, iv := range stored.intervals() {
		covered.add(iv.min, iv.max)
	}

	for _, iv := range unverifiable.intervals() {
		covered.add(iv.min, iv.max)
	}

	for _, r := range authenticated.intervals() {
		for _, gap := range covered.gapsIn(r) {
			reportMissingLogs(gap, callback)
		}
	}

	return nil
}

// maxEnumeratedGapSequences bounds how many individual SEQUENCE_GAP events a
// single missing range may emit. The remainder is collapsed into one summary
// event.
//
// The bound exists because the range's upper edge comes from a stored
// AuditSuccess.max_log_sequence: chain-verified, but forgeable by anyone who
// can write Pebble and knows the cluster ID (the chain is keyed by cluster ID,
// not by a secret). Without it, a crafted max_log_sequence makes Check() hang
// instead of report — a strictly worse outcome than reporting. A store that
// legitimately lost a long trailing run of logs hits the same path benignly.
//
// Deliberately NOT derived from any stored value: bounding this by
// LedgerBoundaries.NextLogId or the highest stored log sequence would gate
// verification on a projection, which is the defect EN-1526 removes.
const maxEnumeratedGapSequences = 1024

// reportMissingLogs emits one SEQUENCE_GAP per sequence in gap, up to
// maxEnumeratedGapSequences, then a single summary SEQUENCE_GAP carrying the
// residual range and its width.
//
// The summary keeps the SEQUENCE_GAP type so it still counts toward errorCount:
// a collapsed report is still a divergence report, and the range it names is
// fully accounted for even though it is not enumerated.
func reportMissingLogs(gap logRange, callback func(*servicepb.CheckStoreEvent)) {
	seq := gap.min

	for range maxEnumeratedGapSequences {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
			fmt.Sprintf("log %d is authenticated by the audit chain but absent from the store", seq),
			seq, "", "", "",
		))

		// Checked here rather than in a loop condition so seq never advances
		// past the maximum uint64.
		if seq == gap.max {
			return
		}

		seq++
	}

	callback(errorEvent(
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
		fmt.Sprintf("logs %d-%d (%d sequences) are authenticated by the audit chain but absent from the store; individual reporting stopped after %d sequences",
			seq, gap.max, gap.max-seq+1, maxEnumeratedGapSequences),
		seq, "", "", "",
	))
}
