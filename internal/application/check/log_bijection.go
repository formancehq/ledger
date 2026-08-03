package check

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
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
