package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/metadata"

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
	var (
		filter       *commonpb.QueryFilter
		scopedLedger string
		scopedKind   string
	)

	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		// Unfiltered.
	case 1:
		scopedLedger = random.RandomChoice(c.ledgerNames)
		filter = filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, scopedLedger)
	default:
		scopedKind = random.RandomChoice([]string{"success", "failure"})
		filter = filterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, scopedKind)
	}

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

		assert.Unreachable("singleton_driver_model: ListAuditEntries returned unexpected error", internal.Details{
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	details := internal.Details{
		"filter":   describeFilter(filter),
		"pageSize": pageSize,
		"reverse":  reverse,
		"rows":     len(entries),
		"seqs":     describeAuditSeqs(entries),
	}

	if violation := auditPageViolation(entries, pageSize, reverse, scopedLedger, scopedKind); violation != "" {
		details["violation"] = violation
		assert.Unreachable("singleton_driver_model: audit page violates its own contract", details)

		return
	}

	verdict := c.validateAuditPage(maxTicket, entries, reverse)
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
func (c *Checker) validateAuditPage(maxTicket uint64, entries []auditEntry, reverse bool) auditVerdict {
	c.mu.Lock()
	defer c.mu.Unlock()

	logs, firstLearned, committedMax := c.committedLogsBySequence()
	unknownBulks := c.bulksOutstandingAt(maxTicket)

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

		latestSeen = max(latestSeen, e.maxLog)
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
				note(row.Sequence, committedLog{ledger: ledger, id: row.ID})
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
func auditPageViolation(entries []auditEntry, pageSize int, reverse bool, scopedLedger, scopedKind string) string {
	if len(entries) > pageSize {
		return "page longer than requested"
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
		}

		if scopedLedger != "" && !slices.Contains(e.ledgers, scopedLedger) {
			return "entry outside the ledger scope"
		}

		if scopedKind == "success" && e.failed {
			return "failure entry in a success-scoped page"
		}

		if scopedKind == "failure" && !e.failed {
			return "success entry in a failure-scoped page"
		}
	}

	return ""
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
