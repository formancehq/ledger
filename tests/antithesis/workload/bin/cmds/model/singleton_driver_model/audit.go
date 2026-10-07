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

	requestedPageSize, pageSize := queryPageSize()
	noteClampedPageSize(requestedPageSize, pageSize)
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	cursor, afterSeq := c.rollAuditCursor()

	malformed, rolled := rollMalformedCursor()
	if rolled {
		cursor, afterSeq = malformed, 0
	}

	c.mu.Lock()
	readID := c.registerRead()
	learnedBefore := c.auditLearnSeq
	c.mu.Unlock()
	defer c.finishRead(readID)

	// A linearizable read is served at a fixed Raft horizon at or past every
	// bulk whose response this driver has observed (EN-1946), so the page stays
	// representable by a candidate base.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListAuditEntries(readCtx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{
			PageSize: uint32(requestedPageSize),
			Cursor:   cursor,
			Reverse:  reverse,
			Filter:   filter,
		},
	})

	var (
		entries []auditEntry
		next    string
	)

	if err == nil {
		raw, drainErr := drainStream(stream)
		err = drainErr

		for _, e := range raw {
			entries = append(entries, auditEntryOf(e))
		}
	}

	if err == nil {
		next = nextCursorOf(stream)
	}

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if handleMalformedCursorError(rolled, "audit", cursor, err) {
			return
		}

		if probe != auditProbeNone && status.Code(err) == codes.InvalidArgument {
			// Coverage: the audit grammar refuses what it cannot express, one
			// literal per refusal — Antithesis catalogues assertions by literal,
			// and each of these is a distinct guard in compileAuditLeaf.
			switch probe {
			case auditProbeSeqInOr:
				assert.Reachable("singleton_driver_model: audit sequence bound inside or rejected", internal.Details{})
			case auditProbeUnspecifiedField:
				assert.Reachable("singleton_driver_model: unspecified audit field rejected", internal.Details{})
			case auditProbeTypeMismatch:
				assert.Reachable("singleton_driver_model: audit condition of the wrong type rejected", internal.Details{"filter": describeFilter(filter)})
			case auditProbeIdempotencyParam:
				assert.Reachable("singleton_driver_model: parameterised idempotency-key condition rejected", internal.Details{})
			case auditProbeNulPrefix:
				assert.Reachable("singleton_driver_model: idempotency-key prefix containing NUL rejected", internal.Details{})
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

	if rolled {
		assert.Unreachable("singleton_driver_model: malformed audit cursor returned results", internal.Details{
			"cursor": cursor,
			"rows":   len(entries),
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
		"filter":     describeFilter(filter),
		"pageSize":   pageSize,
		"requested":  requestedPageSize,
		"reverse":    reverse,
		"cursor":     cursor,
		"nextCursor": next,
		"rows":       len(entries),
		"seqs":       describeAuditSeqs(entries),
	}

	if violation := auditPageViolation(entries, pageSize, reverse, filter, afterSeq, next); violation != "" {
		details["violation"] = violation
		assert.Unreachable("singleton_driver_model: audit page violates its own contract", details)

		return
	}

	verdict := c.validateAuditPage(maxTicket, learnedBefore, entries, reverse, filter, pageSize, afterSeq, next)
	if verdict.finding != "" {
		details["entry"] = verdict.entry
		details["why"] = verdict.why
		if verdict.missingSeq != 0 {
			details["servedAtSeq"] = c.describeServedLogAt(ctx, client, verdict.missingSeq)
		}
		assert.Unreachable("singleton_driver_model: "+verdict.finding, details)

		return
	}

	if entry, why, checked := c.auditIdempotencyViolation(entries); why != "" {
		details["entry"], details["why"] = entry, why
		assert.Unreachable("singleton_driver_model: audit entry names the wrong batch key", details)

		return
	} else if checked > 0 {
		// Coverage: a served entry's batch key was the one the driver sent for
		// the bulk that committed at its log range.
		assert.Reachable("singleton_driver_model: audit entry batch key matched the model", internal.Details{"checked": checked})
	}

	c.noteKnownAuditEntries(entries)

	// Coverage: every entry of the page was the model's own record of a bulk.
	assert.Reachable("singleton_driver_model: audit page validated", internal.Details{"filter": describeFilter(filter)})

	if verdict.rejections > 0 {
		// Coverage: a failed entry matched a rejection the model explained.
		assert.Reachable("singleton_driver_model: audit failure entry matched a model rejection", internal.Details{})
	}

	if verdict.knownCertified > 0 {
		// Coverage: the page was held to entries the trail served on an earlier read.
		assert.Reachable("singleton_driver_model: audit page judged against remembered entries", internal.Details{"certified": verdict.knownCertified})
	}

	if verdict.cursorCorroborated {
		// Coverage: a remembered entry past a full page proved its cursor was owed.
		assert.Reachable("singleton_driver_model: audit resume cursor corroborated by a remembered entry", internal.Details{})
	}
}

// auditEntry is one served entry reduced to what the model can pin.
type auditEntry struct {
	seq        uint64
	proposalID uint64
	timestamp  uint64   // unix microseconds
	subject    string   // caller subject; empty when unauthenticated or system-initiated
	idemKey    string   // batch dedup key, as the entry carries it
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
		subject:    e.GetCallerSnapshot().GetAuthenticated().GetIdentity().GetSubject(),
		idemKey:    e.GetIdempotency().GetKey(),
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
	// knownCertified counts the remembered entries the filter certainly selects
	// on this read, and cursorCorroborated marks the page whose resume cursor one
	// of them proved was owed — the two sondes that say the lower bound armed.
	knownCertified     int
	cursorCorroborated bool
}

// validateAuditPage checks each entry against the committed model. The model
// learns a log's global sequence only when its bulk drains, so bulks still in
// flight at the read's high-water have committed logs the model cannot name
// yet; an entry past the committed frontier is admissible exactly then.
// learnedBefore is c.auditLearnSeq when the read registered. Acquires c.mu.
func (c *Checker) validateAuditPage(maxTicket, learnedBefore uint64, entries []auditEntry, reverse bool, filter *commonpb.QueryFilter, pageSize int, afterSeq uint64, next string) auditVerdict {
	c.mu.Lock()
	defer c.mu.Unlock()

	logs, committedMax := c.committedLogsBySequence()
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
		// Their logs are committed all the same, so a success one still counts
		// toward the tail the page covers.
		if len(e.ledgers) == 0 {
			if !e.failed {
				latestSeen = max(latestSeen, e.maxLog)
			}

			continue
		}

		// Setup committed ledgers and their initial schema before the workers
		// started; the oracle seeded that state directly and learned no sequence
		// for it, so setup's entries are not judged.
		if !e.failed && e.maxLog <= c.setupMaxSeq {
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

		if why, missing := auditSuccessMismatch(e, logs, c.committedBulks, committedMax); why != "" {
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
	// log-sequence range must be under some served success entry. A resumed page
	// starts past its cursor, so the entries covering the earlier logs are on a
	// page this one does not hold.
	if cond := bareAuditLogSeqRange(filter); cond != nil && len(entries) < pageSize && afterSeq == 0 {
		for seq := range logs {
			if !matchUintBounds(cond, seq) {
				continue
			}

			if !slices.ContainsFunc(entries, func(e auditEntry) bool { return !e.failed && e.minLog <= seq && seq <= e.maxLog }) {
				return auditVerdict{finding: "audit trail misses a committed log", entry: "log sequence " + strconv.FormatUint(seq, 10), why: "no served entry covers it", missingSeq: seq}
			}
		}
	}

	// An unfiltered reverse FIRST page starts at the newest entry, so the newest
	// committed bulk must be on it — unless a later rejection or an undrained bulk
	// is the real tail. A resumed page starts below its cursor and says nothing
	// about the tail; a filtered one selects its own newest entry, which
	// auditKnownMatchViolation judges against the entries the trail has shown.
	if reverse && afterSeq == 0 && filter == nil && !unknownBulks && newestSeen && !newestFail && committedMax > 0 && latestSeen < committedMax {
		return auditVerdict{finding: "audit tail misses the newest committed bulk", entry: describeAuditEntry(entries[0]), why: "committed frontier is " + strconv.FormatUint(committedMax, 10) + ", newest covered " + strconv.FormatUint(latestSeen, 10)}
	}

	known := c.auditKnownMatchViolation(learnedBefore, entries, reverse, filter, pageSize, afterSeq, next, logs)
	if known.finding != "" {
		return known
	}

	verdict.knownCertified, verdict.cursorCorroborated = known.knownCertified, known.cursorCorroborated

	return verdict
}

// auditKnownMatchViolation judges a page against the entries the trail has
// already shown this driver. A page is a prefix of the server's own result and
// the known set is a subset of it, so a known match up to the page's last entry
// belongs on the page, and one past it is something the server still owed —
// rows while the page had room, a resume cursor once it was full.
//
// The trail cannot lose an entry between reads: audit history is permanent, and
// a linearizable read takes its barrier at read time, so its snapshot is at or
// past every entry already committed. Caller holds c.mu.
func (c *Checker) auditKnownMatchViolation(learnedBefore uint64, entries []auditEntry, reverse bool, filter *commonpb.QueryFilter, pageSize int, afterSeq uint64, next string, logs map[uint64]committedLog) auditVerdict {
	served := make(map[uint64]bool, len(entries))
	for _, e := range entries {
		served[e.seq] = true
	}

	var last uint64
	if len(entries) > 0 {
		last = entries[len(entries)-1].seq
	}

	var out auditVerdict

	for seq, known := range c.knownAudit {
		// An entry learned after this read registered may have committed past
		// the read's snapshot.
		if known.learned > learnedBefore {
			continue
		}

		k := known.entry
		if afterSeq != 0 && (!reverse && seq <= afterSeq || reverse && seq >= afterSeq) {
			continue // before the cursor: not this page's business
		}

		if !auditEntryCertainlyMatches(filter, k, logs) {
			continue
		}

		out.knownCertified++

		onPage := len(entries) > 0 && (!reverse && seq <= last || reverse && seq >= last)
		switch {
		case onPage:
			if !served[seq] {
				return auditVerdict{finding: "audit page omits an entry the trail served before", entry: describeAuditEntry(k), why: "inside the page's own range", missingSeq: seq}
			}
		case len(entries) < pageSize:
			return auditVerdict{finding: "audit page stopped short of an entry the trail served before", entry: describeAuditEntry(k), why: strconv.Itoa(len(entries)) + " of " + strconv.Itoa(pageSize) + " rows used", missingSeq: seq}
		case next == "":
			return auditVerdict{finding: "audit page dropped its resume cursor", entry: describeAuditEntry(k), why: "a full page left this entry unserved and named no next cursor", missingSeq: seq}
		default:
			out.cursorCorroborated = true
		}
	}

	return out
}

// auditEntryCertainlyMatches reports whether the filter certainly selects e.
// Every leaf but order-type is judged exactly off the entry's own fields; an
// order-type leaf holds only for the kinds the model committed under the entry's
// logs, so an entry whose logs the model does not know — a rejection, a
// system-scoped proposal — certifies nothing. A Not certifies nothing either:
// the audit grammar refuses it, so no served page carries one, and inverting a
// conservative verdict would not stay conservative.
func auditEntryCertainlyMatches(filter *commonpb.QueryFilter, e auditEntry, logs map[uint64]committedLog) bool {
	types := committedOrderTypes(e, logs)

	return foldFilter(filter, filterFold[bool]{
		and: allOf,
		or:  anyOf,
		not: func(bool) bool { return false },
		leaf: func(leaf *commonpb.QueryFilter) bool {
			if a := leaf.GetAudit(); a.GetField() == commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE {
				return types[a.GetStringCond().GetHardcoded()]
			}

			return auditEntrySatisfies(leaf, e)
		},
	})
}

// noteKnownAuditEntries remembers a validated page. Eviction only weakens the
// lower bound, never the other way, so the set is capped. Acquires c.mu.
func (c *Checker) noteKnownAuditEntries(entries []auditEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.auditLearnSeq++

	for _, e := range entries {
		if len(c.knownAudit) >= knownAuditCap {
			if _, held := c.knownAudit[e.seq]; !held {
				for seq := range c.knownAudit {
					delete(c.knownAudit, seq)

					break
				}
			}
		}

		if _, held := c.knownAudit[e.seq]; !held {
			c.knownAudit[e.seq] = knownAuditEntry{entry: e, learned: c.auditLearnSeq}
		}
	}
}

// knownAuditEntry is a remembered audit entry and the auditLearnSeq batch that
// first served it.
type knownAuditEntry struct {
	entry   auditEntry
	learned uint64
}

const knownAuditCap = 4096

// committedLog is one committed log a sequence was learned for; id is zero for
// a ledger-level log, which the oracle keeps no row for.
type committedLog struct {
	ledger string
	id     uint64
	kind   string // the oracle's log kind; empty for a ledger-level log
}

// committedLogsBySequence indexes every committed log a sequence was learned
// for — the oracle's rows and the ledger-level logs it keeps no row for — with
// the highest such sequence. Caller holds c.mu.
func (c *Checker) committedLogsBySequence() (logs map[uint64]committedLog, maxSeq uint64) {
	logs = make(map[uint64]committedLog, len(c.committedLogs))
	note := func(seq uint64, l committedLog) {
		logs[seq] = l
		maxSeq = max(maxSeq, seq)
	}

	for seq, l := range c.committedLogs {
		note(seq, l)
	}

	// The ledger-level record carries the order that produced the log, which the
	// served payload does not name.
	for seq, rec := range c.ledgerLogSeqs {
		note(seq, committedLog{ledger: rec.ledger, kind: rec.kind})
	}

	return logs, maxSeq
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
func auditSuccessMismatch(e auditEntry, logs map[uint64]committedLog, bulks map[uint64]committedBulk, committedMax uint64) (why string, missingSeq uint64) {
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

	// One bulk is one entry, so the range must be some bulk's own boundaries: a
	// contiguous run of committed logs is not enough, or an entry merging two
	// adjacent bulks — or naming part of one — passes every check above.
	bulk, recorded := bulks[e.minLog]
	if !recorded {
		return "no committed bulk begins at " + strconv.FormatUint(e.minLog, 10), e.minLog
	}

	if bulk.maxSeq != e.maxLog {
		return "the bulk at " + strconv.FormatUint(e.minLog, 10) + " ends at " + strconv.FormatUint(bulk.maxSeq, 10) + ", entry names " + strconv.FormatUint(e.maxLog, 10), 0
	}

	return "", 0
}

// auditPageViolation reports the first way the page breaks its own contract —
// length, order, or filter scope — or "" when it holds. These are extras over
// the model check, never the verdict on their own.
func auditPageViolation(entries []auditEntry, pageSize int, reverse bool, filter *commonpb.QueryFilter, afterSeq uint64, next string) string {
	if len(entries) > pageSize {
		return "page longer than requested"
	}

	// The trail's universe is wider than the model's — system-scoped and
	// setup-era entries are not tracked — so no model verdict on whether another
	// entry is waiting is available here. What still holds is structural: the
	// token names the last entry served, and only a full page can carry one.
	if !nextCursorLegal(next, cursorEither, lastAuditKey(entries), len(entries), pageSize) {
		return "resume token does not match the page it rode with"
	}

	// Without an indexed leaf the page is a zone scan, and the zone is dense:
	// every proposal, accepted or rejected, takes the next sequence.
	dense := auditFilterIndexFree(filter)

	// Resume is exclusive and runs in the iteration's own direction.
	for _, e := range entries {
		if afterSeq != 0 && (!reverse && e.seq <= afterSeq || reverse && e.seq >= afterSeq) {
			return "entry outside the cursor"
		}
	}

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

		if i == 0 && dense && !reverse && e.seq != max(auditZoneStart(filter), afterSeq+1) {
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
			case commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY:
				// The only audit field with a prefix form. An entry carrying no
				// key is absent from the index, so neither form selects it.
				if e.idemKey == "" {
					return false
				}

				if p, isPrefix := a.GetCondition().(*commonpb.AuditCondition_StringPrefix); isPrefix {
					return strings.HasPrefix(e.idemKey, p.StringPrefix)
				}

				return e.idemKey == value
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
	"saved_ledger_metadata":            "save_ledger_metadata",
	"deleted_ledger_metadata":          "delete_ledger_metadata",
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

// lastAuditKey is the cursor an audit page implies: the sequence of its last
// entry in decimal, empty for a page that showed none.
func lastAuditKey(entries []auditEntry) string {
	if len(entries) == 0 {
		return ""
	}

	return strconv.FormatUint(entries[len(entries)-1].seq, 10)
}

// rollAuditCursor picks a resume token for an audit page half the time: a
// sequence the trail served earlier, a few below it, so the skip lands inside
// the zone instead of past its head. Acquires c.mu.
func (c *Checker) rollAuditCursor() (string, uint64) {
	if !oneIn(2) {
		return "", 0
	}

	c.mu.Lock()
	var sample auditSample
	if len(c.auditSamples) > 0 {
		sample = c.auditSamples[internal.Rand().Intn(len(c.auditSamples))]
	}
	c.mu.Unlock()

	seq := sample.seq
	seq -= min(seq, internal.Rand().Uint64()%8)

	if seq == 0 {
		return "", 0
	}

	return strconv.FormatUint(seq, 10), seq
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
	// The unspecified field names no index.
	auditProbeUnspecifiedField
	// A string condition on a uint field, or the reverse.
	auditProbeTypeMismatch
	// The idempotency-key index is resolved without a parameter context.
	auditProbeIdempotencyParam
	// A NUL in a prefix operand cannot be encoded as an index bound.
	auditProbeNulPrefix
)

// auditIdempotencyViolation names the first served entry whose batch key is not
// the one the driver sent for the bulk that committed at that log range, with
// why. Only keyed bulks can be placed: the replay registry is the model's record
// of which key went with which committed sequences, and an ephemeral key is
// minted inside applyRequest and never retained. Acquires c.mu.
func (c *Checker) auditIdempotencyViolation(entries []auditEntry) (entry, why string, checked int) {
	c.mu.Lock()
	keyByFirstSeq := make(map[uint64]string, len(c.replayable))
	for _, r := range c.replayable {
		if first := firstNonZero(r.logSeqs); first != 0 {
			keyByFirstSeq[first] = r.key
		}
	}
	c.mu.Unlock()

	for _, e := range entries {
		if e.failed || e.minLog == 0 {
			continue
		}

		key, known := keyByFirstSeq[e.minLog]
		if !known {
			continue
		}

		checked++

		if e.idemKey != key {
			return describeAuditEntry(e), "entry carries key " + e.idemKey + " but that log range was committed under " + key, checked
		}
	}

	return "", "", checked
}

// firstNonZero is the smallest non-zero sequence a committed bulk was assigned,
// which is where its audit entry's log range starts.
func firstNonZero(seqs []uint64) uint64 {
	var first uint64
	for _, seq := range seqs {
		if seq != 0 && (first == 0 || seq < first) {
			first = seq
		}
	}

	return first
}

// auditSample is one served entry's indexed fields, kept so later filters can
// be aimed at values the trail holds.
type auditSample struct {
	seq, proposalID, timestamp uint64
	subject                    string
	idemKey                    string
}

const auditSampleCap = 64

// noteAuditSamples remembers the indexed fields of a served page. Acquires c.mu.
func (c *Checker) noteAuditSamples(entries []auditEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, e := range entries {
		sample := auditSample{seq: e.seq, proposalID: e.proposalID, timestamp: e.timestamp, subject: e.subject, idemKey: e.idemKey}
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
	state := c.modelState
	c.mu.Unlock()

	logSeq, _, _ := pickLogSequence(state)

	leaf := func() *commonpb.QueryFilter { return genAuditLeaf(c.ledgerNames, sample, logSeq.sequence) }
	indexed := func() *commonpb.QueryFilter {
		for {
			if f := leaf(); f.GetAudit().GetField() != commonpb.AuditField_AUDIT_FIELD_SEQUENCE {
				return f
			}
		}
	}

	// Each arm is its own roll against the rolls that fell through above it, so
	// two arms guarding on the same odds are two independent draws, not the
	// repeated constant dupCase reads them as.
	switch {
	case oneIn(6):
		return nil, auditProbeNone
	case oneIn(8):
		if oneIn(2) {
			return filterNot(leaf()), auditProbeNot
		}

		return filterOr(filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, satisfiableSeqRange(sample.seq)), indexed()), auditProbeSeqInOr
	case oneIn(4):
		// One slot of the read mix reaches this read at all, so a refusal rolled
		// much rarer than this is never drawn in a whole local run — the four
		// arms below split whatever share this branch gets. Ordered after the
		// grammar refusals so it does not take theirs.
		return genRefusedAuditLeaf(sample)
	case oneIn(4): //nolint:gocritic // dupCase: a fresh roll, not the arm above
		return filterAnd(genChildren(maxQueryGenDepth, func(int) *commonpb.QueryFilter { return leaf() })...), auditProbeNone
	case oneIn(6): //nolint:gocritic // dupCase: a fresh roll, not the first arm
		return filterOr(indexed(), indexed()), auditProbeNone
	default:
		return leaf(), auditProbeNone
	}
}

// genAuditLeaf rolls one audit leaf, aimed at a served entry's values so ranges
// straddle live entries: the model's committed log sequences for
// log_sequence, the fleet for ledger, the indexer's tokens for order_type.
func genAuditLeaf(ledgers []string, sample auditSample, logSeq uint64) *commonpb.QueryFilter {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8}) {
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
	case 7:
		return genIdempotencyKeyLeaf(sample)
	default:
		token := random.RandomChoice(auditOrderTypes)
		if oneIn(8) {
			token = "no_such_order"
		}

		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, token)
	}
}

// genIdempotencyKeyLeaf rolls the one audit field with both an exact and a
// prefix form. The prefix arm alternates between the driver's own key namespace
// — which every entry this run produced shares, so the page is unfiltered in
// practice — and a proper prefix of a key the trail served, which selects one.
func genIdempotencyKeyLeaf(sample auditSample) *commonpb.QueryFilter {
	key := sample.idemKey
	if key == "" || oneIn(8) {
		key = "model-no-such-key"
	}

	if !oneIn(2) {
		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY, key)
	}

	prefix := idempotencyKeyNamespace
	if oneIn(2) && len(key) > len(idempotencyKeyNamespace) {
		prefix = key[:len(idempotencyKeyNamespace)+internal.Rand().Intn(len(key)-len(idempotencyKeyNamespace))]
	}

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
		Field:     commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
		Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: prefix},
	}}}
}

// genRefusedAuditLeaf rolls a leaf the audit compiler must refuse, with the
// refusal it is built to draw. Each is a distinct guard in compileAuditLeaf.
func genRefusedAuditLeaf(sample auditSample) (*commonpb.QueryFilter, auditProbe) {
	switch random.RandomChoice([]uint8{0, 1, 2, 3}) {
	case 0:
		return filterAuditString(commonpb.AuditField_AUDIT_FIELD_UNSPECIFIED, "anything"), auditProbeUnspecifiedField
	case 1:
		// A string operand on a uint field, or a numeric one on a string field.
		if oneIn(2) {
			return filterAuditString(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, "not-a-number"), auditProbeTypeMismatch
		}

		return filterAuditUint(commonpb.AuditField_AUDIT_FIELD_LEDGER, uintRangeAround(sample.seq, 16)), auditProbeTypeMismatch
	case 2:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
			Field: commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
			Condition: &commonpb.AuditCondition_StringCond{StringCond: &commonpb.StringCondition{
				Value: &commonpb.StringCondition_Param{Param: "p0"},
			}},
		}}}, auditProbeIdempotencyParam
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: &commonpb.AuditCondition{
			Field:     commonpb.AuditField_AUDIT_FIELD_IDEMPOTENCY_KEY,
			Condition: &commonpb.AuditCondition_StringPrefix{StringPrefix: "model-\x00bad"},
		}}}, auditProbeNulPrefix
	}
}

// satisfiableSeqRange is a two-sided sequence range that certainly holds some
// value: closed on both sides, neither side an extremum, lo <= hi. The
// seq-inside-Or refusal only fires for a bound that compiles to a zone bound —
// an IMPOSSIBLE range instead compiles to an empty narrowed set, which the union
// represents happily (compileAuditSeqBound), so the Or would then serve a page.
// A probe built on an arbitrary range is therefore not a probe.
func satisfiableSeqRange(center uint64) *commonpb.UintCondition {
	lo := center - min(center, internal.Rand().Uint64()%16)
	hi := lo + internal.Rand().Uint64()%16

	return &commonpb.UintCondition{Min: &lo, Max: &hi}
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
		if rec, ok := c.ledgerLogSeqs[seq]; ok {
			return served + "; model: ledger-level log of " + rec.ledger
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

// runAuditEntryRead fetches one entry by sequence. It is the only path that
// carries an entry's per-order items, so it is what makes the item checks in
// auditSuccessMismatch reachable at all — a listing leaves items empty. A
// sequence past anything the trail can hold must resolve NotFound.
func runAuditEntryRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	seq, absent := c.pickAuditSequence()
	if seq == 0 {
		return
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	served, err := client.GetAuditEntry(readCtx, &servicepb.GetAuditEntryRequest{Sequence: seq})

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if absent && status.Code(err) == codes.NotFound {
			// Coverage: a sequence the trail cannot hold resolves NotFound.
			assert.Reachable("singleton_driver_model: audit entry read on an unassigned sequence returned NotFound", internal.Details{})

			return
		}

		assert.Unreachable("singleton_driver_model: GetAuditEntry returned unexpected error", internal.Details{
			"sequence": seq,
			"absent":   absent,
			"error":    err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: audit entry served an unassigned sequence", internal.Details{"sequence": seq})

		return
	}

	e := auditEntryOf(served)
	if e.seq != seq {
		assert.Unreachable("singleton_driver_model: audit entry read served a different sequence", internal.Details{
			"asked":  seq,
			"served": e.seq,
		})

		return
	}

	if verdict := c.validateAuditEntry(maxTicket, e); verdict.finding != "" {
		assert.Unreachable("singleton_driver_model: "+verdict.finding, internal.Details{
			"sequence": seq,
			"entry":    verdict.entry,
			"why":      verdict.why,
		})

		return
	}

	c.noteKnownAuditEntries([]auditEntry{e})

	// Coverage: a directly-fetched entry was the model's own record of a bulk.
	assert.Reachable("singleton_driver_model: audit entry read validated", internal.Details{})

	if len(e.itemSeqs) > 0 {
		// Coverage: the per-order items only this path serves were judged.
		assert.Reachable("singleton_driver_model: audit entry carried its per-order items", internal.Details{"orders": len(e.itemSeqs)})
	}
}

// pickAuditSequence chooses a sequence to fetch: one the trail served earlier,
// or — one read in four — a sequence no audit zone can reach. Acquires c.mu.
func (c *Checker) pickAuditSequence() (seq uint64, absent bool) {
	if oneIn(4) {
		return math.MaxUint64, true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.auditSamples) == 0 {
		return 0, false
	}

	return c.auditSamples[internal.Rand().Intn(len(c.auditSamples))].seq, false
}

// validateAuditEntry holds one directly-fetched entry to the rules a page's
// entry is held to, minus the page-shaped ones: a single entry says nothing
// about what else the trail holds, so neither the zone's density nor the
// remembered set applies. Acquires c.mu.
func (c *Checker) validateAuditEntry(maxTicket uint64, e auditEntry) auditVerdict {
	c.mu.Lock()
	defer c.mu.Unlock()

	logs, committedMax := c.committedLogsBySequence()
	unknownBulks := c.bulksOutstandingAt(maxTicket)

	// System-scoped and setup-era entries are not modelled, the same exemptions
	// validateAuditPage grants them.
	if len(e.ledgers) == 0 || !e.failed && e.maxLog <= c.setupMaxSeq {
		return auditVerdict{}
	}

	if e.failed {
		if c.rejectionExplains(e) || unknownBulks {
			return auditVerdict{}
		}

		return auditVerdict{finding: "audit failure entry unexplained by the model", entry: describeAuditEntry(e), why: "no recorded rejection with these ledgers, order count and reason"}
	}

	if e.minLog > committedMax {
		if unknownBulks {
			return auditVerdict{}
		}

		return auditVerdict{finding: "audit entry names logs the model never committed", entry: describeAuditEntry(e), why: "committed frontier is " + strconv.FormatUint(committedMax, 10)}
	}

	if why, missing := auditSuccessMismatch(e, logs, c.committedBulks, committedMax); why != "" {
		return auditVerdict{finding: "audit entry outside model", entry: describeAuditEntry(e), why: why, missingSeq: missing}
	}

	return auditVerdict{}
}
