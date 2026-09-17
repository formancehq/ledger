package main

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runAuditQuery exercises ListAuditEntries with the audit filter leaves and
// checks every served entry against the model. A successful entry names the
// log sequences its bulk committed; the model learned each committed log's
// sequence at commit, so the entry must name exactly one bulk's logs, in the
// ledgers those logs belong to. A failed entry names ledgers and a reason; it
// must be a rejection the model explained. Entries for bulks the model has not
// yet drained are admissible only while such bulks are in flight.
func runAuditQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	filter, probe := c.genAuditFilter()

	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// A linearizable read is served at a fixed Raft horizon at or past every
	// bulk whose response this driver has observed (EN-1946), so the page stays
	// representable by a candidate base.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListAuditEntries(readCtx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Reverse:  reverse,
			Filter:   filter,
		},
	})

	var entries []auditEntry
	if err == nil {
		raw, drainErr := drainStream(stream)
		err = drainErr

		for _, e := range raw {
			entries = append(entries, auditEntryOf(e))
		}
	}

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if probe != auditProbeNone && status.Code(err) == codes.InvalidArgument {
			// Coverage: the audit grammar refuses what its seq-set representation
			// cannot express, one literal per refusal.
			switch probe {
			case auditProbeSeqInOr:
				assert.Reachable("singleton_driver_model: audit sequence bound inside or rejected", internal.Details{})
			default:
				assert.Reachable("singleton_driver_model: audit not filter rejected", internal.Details{})
			}

			return
		}

		assert.Unreachable("singleton_driver_model: ListAuditEntries returned unexpected error", internal.Details{
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if probe != auditProbeNone {
		assert.Unreachable("singleton_driver_model: unsupported audit filter returned a page", internal.Details{
			"filter": describeFilter(filter),
			"rows":   len(entries),
		})

		return
	}

	c.noteAuditSamples(entries)

	details := internal.Details{
		"filter":   describeFilter(filter),
		"pageSize": pageSize,
		"reverse":  reverse,
		"rows":     len(entries),
		"seqs":     describeAuditSeqs(entries),
	}

	if violation := auditPageViolation(entries, pageSize, reverse, filter); violation != "" {
		details["violation"] = violation
		assert.Unreachable("singleton_driver_model: audit page violates its own contract", details)

		return
	}

	verdict := c.validateAuditPage(maxTicket, entries, reverse, filter, pageSize)
	if verdict.finding != "" {
		details["entry"] = verdict.entry
		details["why"] = verdict.why
		if verdict.missingSeq != 0 {
			details["servedAtSeq"] = c.describeServedLogAt(ctx, client, verdict.missingSeq)
		}
		assert.Unreachable("singleton_driver_model: "+verdict.finding, details)

		return
	}

	// Coverage: every entry of the page was the model's own record of a bulk.
	assert.Reachable("singleton_driver_model: audit page validated", internal.Details{"filter": describeFilter(filter)})

	if verdict.rejections > 0 {
		// Coverage: a failed entry matched a rejection the model explained.
		assert.Reachable("singleton_driver_model: audit failure entry matched a model rejection", internal.Details{})
	}
}

// auditEntry is one served entry reduced to what the model can pin.
type auditEntry struct {
	seq        uint64
	proposalID uint64
	timestamp  uint64   // unix microseconds
	subject    string   // caller subject; empty when unauthenticated or system-initiated
	ledgers    []string // as served, ascending
	orderCount uint32
	failed     bool
	reason     string   // failure only, in the domain's vocabulary
	minLog     uint64   // success only
	maxLog     uint64   // success only
	itemSeqs   []uint64 // success only, one per order; empty once archived
}

func auditEntryOf(e *auditpb.AuditEntry) auditEntry {
	out := auditEntry{
		seq:        e.GetSequence(),
		proposalID: e.GetProposalId(),
		timestamp:  e.GetTimestamp().GetData(),
		subject:    e.GetCallerSnapshot().GetIdentity().GetSubject(),
		ledgers:    slices.Sorted(slices.Values(e.GetLedgers())),
		orderCount: e.GetOrderCount(),
	}

	if f := e.GetFailure(); f != nil {
		out.failed = true
		out.reason = strings.TrimPrefix(f.GetReason().String(), "ERROR_REASON_")

		return out
	}

	out.minLog = e.GetSuccess().GetMinLogSequence()
	out.maxLog = e.GetSuccess().GetMaxLogSequence()

	for _, item := range e.GetItems() {
		out.itemSeqs = append(out.itemSeqs, item.GetLogSequence())
	}

	return out
}

// auditVerdict is validateAuditPage's outcome: the finding class and the entry
// that produced it, or the number of failed entries a recorded rejection
// explained when the page holds.
type auditVerdict struct {
	finding    string
	entry      string
	why        string
	missingSeq uint64 // the sequence the model lacked, for the finding's diagnostics
	rejections int
}

// validateAuditPage checks each entry against the committed model. The model
// learns a log's global sequence only when its bulk drains, so bulks still in
// flight at the read's high-water have committed logs the model cannot name
// yet; an entry past the committed frontier is admissible exactly then.
// Acquires c.mu.
func (c *Checker) validateAuditPage(maxTicket uint64, entries []auditEntry, reverse bool, filter *commonpb.QueryFilter, pageSize int) auditVerdict {
	c.mu.Lock()
	defer c.mu.Unlock()

	logs, firstLearned, committedMax := c.committedLogsBySequence()
	unknownBulks := c.bulksOutstandingAt(maxTicket)
	typed := auditFiltersOrderType(filter)

	var (
		verdict    auditVerdict
		latestSeen uint64 // highest committed sequence a served success entry covers
		newestSeen bool   // a ledger-scoped entry has been visited
		newestFail bool   // the newest ledger-scoped entry on a reverse page is a rejection
	)

	for _, e := range entries {
		// System-scoped orders (query checkpoints and the like) touch no ledger
		// and are not modelled; their entries are neither predicted nor denied.
		if len(e.ledgers) == 0 {
			continue
		}

		// Setup committed ledgers and their initial schema before the first bulk
		// this driver drained; the oracle seeded that state directly and learned no
		// sequence for it, so entries below the first learned one are not judged.
		if !e.failed && e.maxLog < firstLearned {
			continue
		}

		if !newestSeen {
			newestSeen = true
			newestFail = e.failed
		}

		if e.failed {
			if c.rejectionExplains(e) {
				verdict.rejections++

				continue
			}

			if unknownBulks {
				continue
			}

			return auditVerdict{finding: "audit failure entry unexplained by the model", entry: describeAuditEntry(e), why: "no recorded rejection with these ledgers, order count and reason"}
		}

		if e.minLog > committedMax {
			if unknownBulks {
				continue
			}

			return auditVerdict{finding: "audit entry names logs the model never committed", entry: describeAuditEntry(e), why: "committed frontier is " + strconv.FormatUint(committedMax, 10), missingSeq: e.minLog}
		}

		if why, missing := auditSuccessMismatch(e, logs, committedMax); why != "" {
			return auditVerdict{finding: "audit entry outside model", entry: describeAuditEntry(e), why: why, missingSeq: missing}
		}

		// The order types the model committed under this entry's logs are the
		// only ones the order-type index can hold for it.
		if typed && !auditEntrySatisfiesTypes(filter, committedOrderTypes(e, logs)) {
			return auditVerdict{finding: "audit entry outside the order-type scope", entry: describeAuditEntry(e), why: "model committed " + strings.Join(slices.Sorted(maps.Keys(committedOrderTypes(e, logs))), ",")}
		}

		latestSeen = max(latestSeen, e.maxLog)
	}

	// A page the size limit did not cut short holds every entry the filter
	// selects, so every committed log the model learned inside a bare
	// log-sequence range must be under some served success entry.
	if cond := bareAuditLogSeqRange(filter); cond != nil && len(entries) < pageSize {
		for seq := range logs {
			if seq < firstLearned || !matchUintBounds(cond, seq) {
				continue
			}

			if !slices.ContainsFunc(entries, func(e auditEntry) bool { return !e.failed && e.minLog <= seq && seq <= e.maxLog }) {
				return auditVerdict{finding: "audit trail misses a committed log", entry: "log sequence " + strconv.FormatUint(seq, 10), why: "no served entry covers it", missingSeq: seq}
			}
		}
	}

	// A reverse first page starts at the newest entry, so the newest committed
	// bulk must be on it — unless a later rejection or an undrained bulk is the
	// real tail.
	if reverse && !unknownBulks && newestSeen && !newestFail && committedMax > 0 && latestSeen < committedMax {
		return auditVerdict{finding: "audit tail misses the newest committed bulk", entry: describeAuditEntry(entries[0]), why: "committed frontier is " + strconv.FormatUint(committedMax, 10) + ", newest covered " + strconv.FormatUint(latestSeen, 10)}
	}

	return verdict
}

// committedLog is one committed log a sequence was learned for; id is zero for
// a ledger-level log, which the oracle keeps no row for.
type committedLog struct {
	ledger string
	id     uint64
	kind   string // the oracle's log kind; empty for a ledger-level log
}

// committedLogsBySequence indexes every committed log a sequence was learned
// for — the oracle's rows and the ledger-level logs it keeps no row for — with
// the lowest and highest such sequence. Caller holds c.mu.
func (c *Checker) committedLogsBySequence() (logs map[uint64]committedLog, minSeq, maxSeq uint64) {
	logs = map[uint64]committedLog{}
	note := func(seq uint64, l committedLog) {
		logs[seq] = l
		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}
		maxSeq = max(maxSeq, seq)
	}

	for _, ledger := range c.ledgerNames {
		for _, row := range c.modelState.Ledger(ledger).LogRows() {
			if row.Sequence != 0 {
				note(row.Sequence, committedLog{ledger: ledger, id: row.ID, kind: row.Kind})
			}
		}
	}

	for seq, ledger := range c.ledgerLogSeqs {
		note(seq, committedLog{ledger: ledger})
	}

	return logs, minSeq, maxSeq
}

// bulksOutstandingAt reports whether any bulk dispatched no later than
// maxTicket has not been drained into the committed model. Caller holds c.mu.
func (c *Checker) bulksOutstandingAt(maxTicket uint64) bool {
	for t := range c.inflight {
		if t <= maxTicket {
			return true
		}
	}

	for _, pe := range c.pending {
		if pe.obs.ticket <= maxTicket {
			return true
		}
	}

	return false
}

// rejectionExplains reports whether a recorded, model-explained rejection has
// this entry's ledgers, order count and reason. Caller holds c.mu.
func (c *Checker) rejectionExplains(e auditEntry) bool {
	shape := rejectedBulk{ledgers: strings.Join(e.ledgers, ","), orders: e.orderCount, reason: e.reason}
	if _, ok := c.rejections[shape]; ok {
		return true
	}

	// A reason the wire enum does not name reaches the trail as UNSPECIFIED.
	if e.reason != "UNSPECIFIED" {
		return false
	}

	for r := range c.rejections {
		if r.ledgers == shape.ledgers && r.orders == shape.orders {
			return true
		}
	}

	return false
}

// auditSuccessMismatch explains why a successful entry within the committed
// frontier is not one bulk of the model, or "" when it is: its sequence range
// is contiguous, one order per log, every sequence is a committed log, and the
// entry's ledgers are exactly those logs' ledgers.
func auditSuccessMismatch(e auditEntry, logs map[uint64]committedLog, committedMax uint64) (why string, missingSeq uint64) {
	if e.minLog == 0 || e.maxLog < e.minLog {
		return "empty or inverted log range", 0
	}

	if e.maxLog > committedMax {
		return "log range straddles the committed frontier", 0
	}

	span := e.maxLog - e.minLog + 1
	if uint64(e.orderCount) != span {
		return "order count " + strconv.FormatUint(uint64(e.orderCount), 10) + " for " + strconv.FormatUint(span, 10) + " logs", 0
	}

	// Items are purged from archived entries; a present list is one per order.
	if len(e.itemSeqs) != 0 && uint64(len(e.itemSeqs)) != span {
		return strconv.Itoa(len(e.itemSeqs)) + " items for " + strconv.FormatUint(span, 10) + " logs", 0
	}

	seen := map[string]bool{}
	for seq := e.minLog; seq <= e.maxLog; seq++ {
		l, ok := logs[seq]
		if !ok {
			return "sequence " + strconv.FormatUint(seq, 10) + " is no committed log", seq
		}

		seen[l.ledger] = true
	}

	for _, seq := range e.itemSeqs {
		if seq < e.minLog || seq > e.maxLog {
			return "item sequence " + strconv.FormatUint(seq, 10) + " outside the entry's range", 0
		}
	}

	ledgers := slices.Sorted(maps.Keys(seen))
	if !slices.Equal(ledgers, e.ledgers) {
		return "logs belong to " + strings.Join(ledgers, ",") + ", entry names " + strings.Join(e.ledgers, ","), 0
	}

	return "", 0
}

// auditPageViolation reports the first way the page breaks its own contract —
// length, order, or filter scope — or "" when it holds. These are extras over
// the model check, never the verdict on their own.
func auditPageViolation(entries []auditEntry, pageSize int, reverse bool, filter *commonpb.QueryFilter) string {
	if len(entries) > pageSize {
		return "page longer than requested"
	}

	// Without an indexed leaf the page is a zone scan, and the zone is dense:
	// every proposal, accepted or rejected, takes the next sequence.
	dense := auditFilterIndexFree(filter)

	for i, e := range entries {
		if i > 0 {
			prev := entries[i-1].seq
			if !reverse && e.seq <= prev {
				return "ascending order violated"
			}

			if reverse && e.seq >= prev {
				return "descending order violated"
			}

			if dense && !reverse && e.seq != prev+1 {
				return "gap in a zone-scan page"
			}

			if dense && reverse && e.seq != prev-1 {
				return "gap in a zone-scan page"
			}
		}

		if i == 0 && dense && !reverse && e.seq != auditZoneStart(filter) {
			return "zone-scan page does not start at its lower bound"
		}

		if !auditEntrySatisfies(filter, e) {
			return "entry outside the filter"
		}
	}

	return ""
}

// auditEntrySatisfies reports whether a served entry meets the filter it was
// selected by, on the entry's own fields. An order-type leaf holds here: the
// list entry does not carry its orders, so validateAuditPage judges it from
// the model's log kinds instead.
func auditEntrySatisfies(filter *commonpb.QueryFilter, e auditEntry) bool {
	return foldFilter(filter, filterFold[bool]{
		and: allOf,
		or:  anyOf,
		not: negate,
		leaf: func(leaf *commonpb.QueryFilter) bool {
			a := leaf.GetAudit()
			if a == nil {
				return leaf.GetFilter() == nil
			}

			value := a.GetStringCond().GetHardcoded()
			switch a.GetField() {
			case commonpb.AuditField_AUDIT_FIELD_SEQUENCE:
				return matchUintBounds(a.GetUintCond(), e.seq)
			case commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID:
				return matchUintBounds(a.GetUintCond(), e.proposalID)
			case commonpb.AuditField_AUDIT_FIELD_TIMESTAMP:
				return matchUintBounds(a.GetUintCond(), e.timestamp)
			case commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE:
				// Match-any over the entry's items, whose sequences are the
				// contiguous range a success entry names; a rejection has none.
				return !e.failed && boundsMeetRange(a.GetUintCond(), e.minLog, e.maxLog)
			case commonpb.AuditField_AUDIT_FIELD_OUTCOME:
				return (value == "failure") == e.failed
			case commonpb.AuditField_AUDIT_FIELD_LEDGER:
				return slices.Contains(e.ledgers, value)
			case commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT:
				// An empty subject is never indexed.
				return e.subject != "" && e.subject == value
			default:
				return true
			}
		},
	})
}

// auditEntrySatisfiesTypes evaluates only the order-type leaves of filter
// against the types the model committed for the entry; every other leaf holds.
func auditEntrySatisfiesTypes(filter *commonpb.QueryFilter, types map[string]bool) bool {
	return foldFilter(filter, filterFold[bool]{
		and: allOf,
		or:  anyOf,
		not: negate,
		leaf: func(leaf *commonpb.QueryFilter) bool {
			a := leaf.GetAudit()
			if a == nil || a.GetField() != commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE {
				return true
			}

			return types[a.GetStringCond().GetHardcoded()]
		},
	})
}

// committedOrderTypes maps the logs a success entry names to the audit
// indexer's order-type tokens. A ledger-level log has no oracle row, so both
// ledger-metadata tokens are admitted for it.
func committedOrderTypes(e auditEntry, logs map[uint64]committedLog) map[string]bool {
	types := map[string]bool{}
	for seq := e.minLog; seq <= e.maxLog && seq >= e.minLog; seq++ {
		l, ok := logs[seq]
		if !ok {
			continue
		}

		if l.id == 0 {
			types["save_ledger_metadata"], types["delete_ledger_metadata"] = true, true

			continue
		}

		if token, known := auditOrderTypeOfKind[l.kind]; known {
			types[token] = true
		}
	}

	return types
}

// auditOrderTypeOfKind maps the oracle's log kinds to domain.AuditOrderType's
// tokens for the orders this workload sends.
var auditOrderTypeOfKind = map[string]string{
	"created_transaction":              "create_transaction",
	"reverted_transaction":             "revert_transaction",
	"saved_metadata":                   "add_metadata",
	"deleted_metadata":                 "delete_metadata",
	"set_metadata_field_type":          "set_metadata_field_type",
	"removed_metadata_field_type":      "remove_metadata_field_type",
	"create_index":                     "create_index",
	"drop_index":                       "drop_index",
	"added_account_type":               "add_account_type",
	"removed_account_type":             "remove_account_type",
	"updated_default_enforcement_mode": "update_default_enforcement_mode",
}

// auditOrderTypes are the tokens a filter may ask for: every kind this
// workload commits plus the two ledger-metadata orders.
var auditOrderTypes = []string{
	"create_transaction", "revert_transaction", "add_metadata", "delete_metadata",
	"set_metadata_field_type", "remove_metadata_field_type", "create_index", "drop_index",
	"add_account_type", "remove_account_type", "update_default_enforcement_mode",
	"save_ledger_metadata", "delete_ledger_metadata",
}

// auditFiltersOrderType reports whether filter carries an order-type leaf.
func auditFiltersOrderType(filter *commonpb.QueryFilter) bool {
	return anyLeaf(filter, func(leaf *commonpb.QueryFilter) bool {
		return leaf.GetAudit().GetField() == commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE
	})
}

// auditFilterIndexFree mirrors the compiler's dispatch: nil, a sequence bound,
// or an And of those scans the zone; anything else goes through the index.
func auditFilterIndexFree(filter *commonpb.QueryFilter) bool {
	return foldFilter(filter, filterFold[bool]{
		and: allOf,
		or:  func([]bool) bool { return false },
		not: func(bool) bool { return false },
		leaf: func(leaf *commonpb.QueryFilter) bool {
			return leaf.GetFilter() == nil || leaf.GetAudit().GetField() == commonpb.AuditField_AUDIT_FIELD_SEQUENCE
		},
	})
}

// auditZoneStart is the first sequence a forward zone scan serves: the zone
// starts at 1 and every sequence bound of the filter raises it.
func auditZoneStart(filter *commonpb.QueryFilter) uint64 {
	start := uint64(1)
	foldFilter(filter, filterFold[struct{}]{
		and: func([]struct{}) struct{} { return struct{}{} },
		or:  func([]struct{}) struct{} { return struct{}{} },
		not: identity[struct{}],
		leaf: func(leaf *commonpb.QueryFilter) struct{} {
			if a := leaf.GetAudit(); a.GetField() == commonpb.AuditField_AUDIT_FIELD_SEQUENCE && a.GetUintCond().Min != nil {
				lo := a.GetUintCond().GetMin()
				if a.GetUintCond().GetMinExclusive() {
					lo++
				}

				start = max(start, lo)
			}

			return struct{}{}
		},
	})

	return start
}

// bareAuditLogSeqRange returns the condition when filter is exactly one
// log-sequence leaf.
func bareAuditLogSeqRange(filter *commonpb.QueryFilter) *commonpb.UintCondition {
	if a := filter.GetAudit(); a != nil && a.GetField() == commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE {
		return a.GetUintCond()
	}

	return nil
}

// boundsMeetRange reports whether some value of [lo, hi] satisfies cond.
func boundsMeetRange(cond *commonpb.UintCondition, lo, hi uint64) bool {
	if cond.Min != nil {
		m := cond.GetMin()
		if cond.GetMinExclusive() {
			if m == math.MaxUint64 {
				return false
			}

			m++
		}

		lo = max(lo, m)
	}

	if cond.Max != nil {
		m := cond.GetMax()
		if cond.GetMaxExclusive() {
			if m == 0 {
				return false
			}

			m--
		}

		hi = min(hi, m)
	}

	return lo <= hi
}

// --- generation ---------------------------------------------------------

// auditProbe names the refusal a generated audit filter is built to draw.
type auditProbe int

const (
	auditProbeNone auditProbe = iota
	// A sequence bound inside Or: the seq-set representation cannot union it.
	auditProbeSeqInOr
	// Not is not valid on the audit target.
	auditProbeNot
)

// auditSample is one served entry's indexed fields, kept so later filters can
// be aimed at values the trail holds.
type auditSample struct {
	seq, proposalID, timestamp uint64
	subject                    string
}

const auditSampleCap = 64

// noteAuditSamples remembers the indexed fields of a served page. Acquires c.mu.
func (c *Checker) noteAuditSamples(entries []auditEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range entries {
		sample := auditSample{seq: e.seq, proposalID: e.proposalID, timestamp: e.timestamp, subject: e.subject}
		if len(c.auditSamples) < auditSampleCap {
			c.auditSamples = append(c.auditSamples, sample)

			continue
		}

		c.auditSamples[internal.Rand().Intn(auditSampleCap)] = sample
	}
}

// genAuditFilter rolls a ListAuditEntries filter: nothing, one leaf on any of
// the eight audit fields, an And or Or of leaves, or one of the two shapes the
// audit grammar refuses. Acquires c.mu.
func (c *Checker) genAuditFilter() (*commonpb.QueryFilter, auditProbe) {
	c.mu.Lock()
	var sample auditSample
	if len(c.auditSamples) > 0 {
		sample = c.auditSamples[internal.Rand().Intn(len(c.auditSamples))]
	}
	c.mu.Unlock()

	logSeq, _, _ := c.pickLogSequence()

	leaf := func() *commonpb.QueryFilter { return genAuditLeaf(c.ledgerNames, sample, logSeq.sequence) }
	indexed := func() *commonpb.QueryFilter {
		for {
			if f := leaf(); f.GetAudit().GetField() != commonpb.AuditField_AUDIT_FIELD_SEQUENCE {
				return f
			}
		}
	}

	switch {
	case oneIn(6):
		return nil, auditProbeNone
	case oneIn(8):
		if oneIn(2) {
			return filterNot(leaf()), auditProbeNot
		}

		return filterOr(filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, uintRangeAround(sample.seq, 16)), indexed()), auditProbeSeqInOr
	case oneIn(4):
		return filterAnd(genChildren(maxQueryGenDepth, func(int) *commonpb.QueryFilter { return leaf() })...), auditProbeNone
	case oneIn(6):
		return filterOr(indexed(), indexed()), auditProbeNone
	default:
		return leaf(), auditProbeNone
	}
}

// genAuditLeaf rolls one audit leaf, aimed at a served entry's values so ranges
// straddle live entries: the model's committed log sequences for
// log_sequence, the fleet for ledger, the indexer's tokens for order_type.
func genAuditLeaf(ledgers []string, sample auditSample, logSeq uint64) *commonpb.QueryFilter {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0:
		ledger := random.RandomChoice(ledgers)
		if oneIn(8) {
			ledger = "no-such-ledger"
		}

		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, ledger)
	case 1:
		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, random.RandomChoice([]string{"success", "failure"}))
	case 2:
		return filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, uintRangeAround(sample.seq, 16))
	case 3:
		return filterAuditUint(commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, uintRangeAround(sample.proposalID, 16))
	case 4:
		// A second either side of a served timestamp, in microseconds.
		return filterAuditUint(commonpb.AuditField_AUDIT_FIELD_TIMESTAMP, uintRangeAround(sample.timestamp, 1_000_000))
	case 5:
		return filterAuditUint(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, uintRangeAround(logSeq, 8))
	case 6:
		subject := sample.subject
		if subject == "" || oneIn(8) {
			subject = random.RandomChoice([]string{"nobody", ""})
		}

		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, subject)
	default:
		token := random.RandomChoice(auditOrderTypes)
		if oneIn(8) {
			token = "no_such_order"
		}

		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, token)
	}
}

// uintRangeAround rolls a two-sided range within spread of center, then opens
// or makes exclusive either side the way the other range leaves do.
func uintRangeAround(center, spread uint64) *commonpb.UintCondition {
	lo := center - min(center, internal.Rand().Uint64()%spread)
	hi := center + internal.Rand().Uint64()%spread
	cond := &commonpb.UintCondition{Min: &lo, Max: &hi}
	rollOpenOrExclusive(cond)

	return cond
}

func filterAuditUint(field commonpb.AuditField, cond *commonpb.UintCondition) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field:     field,
		Condition: &commonpb.AuditCondition_UintCond{UintCond: cond},
	}}}
}

func describeAuditEntry(e auditEntry) string {
	outcome := "ok[" + strconv.FormatUint(e.minLog, 10) + ".." + strconv.FormatUint(e.maxLog, 10) + "]"
	if e.failed {
		outcome = "failed:" + e.reason
	}

	return "seq" + strconv.FormatUint(e.seq, 10) + " " + outcome + " orders=" + strconv.FormatUint(uint64(e.orderCount), 10) + " ledgers=" + strings.Join(e.ledgers, ",")
}

func describeAuditSeqs(entries []auditEntry) string {
	seqs := make([]string, 0, len(entries))
	for _, e := range entries {
		seqs = append(seqs, strconv.FormatUint(e.seq, 10))
	}

	return strings.Join(seqs, ",")
}

// filterAuditString builds an audit leaf on a string-typed audit field.
func filterAuditString(field commonpb.AuditField, value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field: field,
		Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	}}}
}

// describeServedLogAt renders what the server holds at a global sequence next
// to the model's record of that ledger log, for a finding's diagnostics.
// Acquires c.mu.
func (c *Checker) describeServedLogAt(ctx context.Context, client servicepb.BucketServiceClient, seq uint64) string {
	log, err := client.GetLog(metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable"), &servicepb.GetLogRequest{Sequence: seq})
	if err != nil {
		return "GetLog: " + err.Error()
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	apply, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		served := "top-level " + strings.TrimPrefix(fmt.Sprintf("%T", log.GetPayload().GetType()), "*commonpb.LogPayload_")
		if l, ok := c.ledgerLogSeqs[seq]; ok {
			return served + "; model: ledger-level log of " + l
		}

		return served + "; model: unknown sequence"
	}

	ledger, id := apply.Apply.GetLedgerName(), apply.Apply.GetLog().GetId()
	served := "apply ledger=" + ledger + " id=" + strconv.FormatUint(id, 10) + " kind=" + serverLogKind(log)

	rows := c.modelState.Ledger(ledger).LogRows()
	model := "model has " + strconv.Itoa(len(rows)) + " rows"
	if id != 0 && id <= uint64(len(rows)) {
		row := rows[id-1]
		model += ", row " + strconv.FormatUint(id, 10) + " = " + row.Kind + "@seq" + strconv.FormatUint(row.Sequence, 10)
	}

	return served + "; " + model
}
