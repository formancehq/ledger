package main

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LOGS is the only target whose universe comes from the read index rather than
// the main store, so it is the one shape that is always owed cross-store
// alignment (query.AlignmentOwed). Nothing else in the driver reaches it.
//
// Two of the three LOGS-valid leaves need no index: the ledger name and the
// ledger-local log id. The third, the log date, is served from an opt-in
// builtin index, so a filter carrying one is owed whatever that index's
// lifecycle in the model allows — a page or a not-ready refusal
// (neededLogIndexes, validateLogQuery).

// genLogFilter builds a filter over the conditions valid on LOGS: ledger name,
// log id range, and the boolean combinators. Returns nil for the unfiltered
// case, which exercises the universe scan itself.
func genLogFilter(ledger string, depth int) *commonpb.QueryFilter {
	if depth >= 2 || oneIn(3) {
		return genLogLeaf(ledger)
	}

	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		return filterAnd(genLogFilter(ledger, depth+1), genLogFilter(ledger, depth+1))
	case 1:
		return filterOr(genLogFilter(ledger, depth+1), genLogFilter(ledger, depth+1))
	default:
		return filterNot(genLogFilter(ledger, depth+1))
	}
}

// genLogLeaf picks one LOGS-valid leaf. Bounds straddle the populated range so
// empty, partial and total windows all occur.
func genLogLeaf(ledger string) *commonpb.QueryFilter {
	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		name := ledger
		if oneIn(4) {
			name = "no-such-ledger"
		}

		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{
			Ledger: &commonpb.LedgerCondition{
				Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: name}},
			},
		}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
			LogId: &commonpb.LogIdCondition{Cond: genLogUintCond()},
		}}
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
			LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
				Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
				Cond:  genLogUintCond(),
			},
		}}
	}
}

func genLogUintCond() *commonpb.UintCondition {
	cond := &commonpb.UintCondition{}

	if oneIn(2) {
		min := internal.Rand().Uint64() % 32
		cond.Min = &min
		cond.MinExclusive = oneIn(2)
	}

	if oneIn(2) {
		max := 8 + internal.Rand().Uint64()%64
		cond.Max = &max
		cond.MaxExclusive = oneIn(2)
	}

	return cond
}

// hasDateLeaf reports whether the filter reads the log date, which is served
// only when the log-date builtin index exists.
func hasDateLeaf(f *commonpb.QueryFilter) bool {
	return anyLeaf(f, func(leaf *commonpb.QueryFilter) bool {
		_, ok := leaf.GetFilter().(*commonpb.QueryFilter_LogBuiltinUint)

		return ok
	})
}

// logDateIndexID is the opt-in builtin index the log date is served from.
// logDateIndexCanonical is its stable map key in the model's index set.
func logDateIndexID() *commonpb.IndexID {
	return indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
}

var logDateIndexCanonical = indexes.Canonical(logDateIndexID())

// neededLogIndexes returns the canonical IDs of the indexes a LOGS filter
// needs the compiler to find ready. The model owns whether each one exists:
// the outcome the server owes — a page or a not-ready refusal — follows from
// the index's lifecycle in every candidate base, exactly as on the accounts
// and transactions paths.
func neededLogIndexes(f *commonpb.QueryFilter) map[string]struct{} {
	if !hasDateLeaf(f) {
		return nil
	}

	return map[string]struct{}{logDateIndexCanonical: {}}
}

// matchLogFilter evaluates a LOGS filter against one modelled log, three-valued
// (see kleene). date is the server-assigned value learned at commit; it is nil
// for a log whose response this base has not folded yet — a candidate base that
// applied an observed-but-undrained bulk holds exactly such logs — so a date
// leaf reading it is undecided, and the row becomes optional in the predicted
// window rather than absent from it.
func matchLogFilter(ledger string, id uint64, date *commonpb.Timestamp, f *commonpb.QueryFilter) (match, known bool) {
	v := foldFilter(f, filterFold[kleene]{
		and: kleeneAnd,
		or:  kleeneOr,
		not: kleeneNot,
		leaf: func(leaf *commonpb.QueryFilter) kleene {
			switch t := leaf.GetFilter().(type) {
			case nil:
				return kleene{match: true, known: true}
			case *commonpb.QueryFilter_Ledger:
				return kleene{match: ledger == t.Ledger.GetCond().GetHardcoded(), known: true}
			case *commonpb.QueryFilter_LogId:
				return kleene{match: matchUintBounds(t.LogId.GetCond(), id), known: true}
			case *commonpb.QueryFilter_LogBuiltinUint:
				if date == nil {
					return kleene{known: false}
				}

				return kleene{match: matchUintBounds(t.LogBuiltinUint.GetCond(), date.GetData()), known: true}
			default:
				// Every LOGS-valid condition is handled above; anything else means
				// the generator and the matcher have drifted apart.
				panic("model: unmatched LOGS condition")
			}
		},
	})

	return v.match, v.known
}

// logWindowRow is one row a ListLogs page may draw, with the fields the model
// pins on it. required=false marks a log whose date this base has not learned,
// so a date leaf cannot be decided for it: the page may or may not carry it.
type logWindowRow struct {
	id        uint64
	kind      string
	payload   string
	date      *commonpb.Timestamp
	sequence  uint64
	purged    string
	newKept   string
	ephemeral string
	required  bool
}

// serverLogRow is one log of a page, reduced to what the model can pin: the
// ledger it belongs to, its per-ledger id, its payload kind, the date the
// server assigned it, its end-of-bulk volume annotations, and whether it
// arrived signed. volumesKnown is false when an annotation names a colour, a
// dimension the model's volume key does not carry.
type serverLogRow struct {
	ledger       string
	id           uint64
	kind         string
	payload      string
	date         uint64
	hasDate      bool
	sequence     uint64
	purged       string
	newKept      string
	ephemeral    string
	volumesKnown bool
	signed       bool
}

// serverLogRows reads a page into comparable rows.
func serverLogRows(logs []*commonpb.Log) []serverLogRow {
	out := make([]serverLogRow, 0, len(logs))

	for _, l := range logs {
		entry := l.GetPayload().GetApply().GetLog()

		purged, purgedOK := renderServedVolumes(entry.GetPurgedVolumes())
		newKept, newKeptOK := renderServedVolumes(entry.GetNewKeptVolumes())
		ephemeral, ephemeralOK := renderServedVolumes(entry.GetEphemeralVolumes())

		out = append(out, serverLogRow{
			ledger:       l.GetPayload().GetApply().GetLedgerName(),
			id:           entry.GetId(),
			kind:         serverLogKind(l),
			payload:      oracle.CanonicalServedLogPayload(entry.GetData()),
			date:         entry.GetDate().GetData(),
			hasDate:      entry.GetDate() != nil,
			sequence:     l.GetSequence(),
			purged:       purged,
			newKept:      newKept,
			ephemeral:    ephemeral,
			volumesKnown: purgedOK && newKeptOK && ephemeralOK,
			signed:       l.GetResponseSignature() != nil,
		})
	}

	return out
}

// renderServedVolumes names a served annotation list the way the oracle names
// its own: "account:asset" entries joined by commas, verbatim in the order the
// server sent them, so a mis-sorted or duplicated list is a mismatch. The
// second result is false when an entry carries a colour.
func renderServedVolumes(vols []*commonpb.TouchedVolume) (string, bool) {
	if len(vols) == 0 {
		return "", true
	}

	parts := make([]string, 0, len(vols))
	for _, v := range vols {
		if v.GetColor() != "" {
			return "", false
		}

		parts = append(parts, v.GetAccount()+":"+v.GetAsset())
	}

	return strings.Join(parts, ","), true
}

// serverLogKind names a log's payload the way the model names it from the
// request that produced it (oracle logKindOf), so the two are comparable.
func serverLogKind(l *commonpb.Log) string {
	switch d := l.GetPayload().GetApply().GetLog().GetData(); {
	case d.GetCreatedTransaction() != nil:
		return "created_transaction"
	case d.GetRevertedTransaction() != nil:
		return "reverted_transaction"
	case d.GetSavedMetadata() != nil:
		return "saved_metadata"
	case d.GetDeletedMetadata() != nil:
		return "deleted_metadata"
	case d.GetSetMetadataFieldType() != nil:
		return "set_metadata_field_type"
	case d.GetRemovedMetadataFieldType() != nil:
		return "removed_metadata_field_type"
	case d.GetCreateIndex() != nil:
		return "create_index"
	case d.GetDropIndex() != nil:
		return "drop_index"
	case d.GetAddedAccountType() != nil:
		return "added_account_type"
	case d.GetRemovedAccountType() != nil:
		return "removed_account_type"
	default:
		return "other"
	}
}

// logWindowRows is the ordered, cursor-filtered, UNTRUNCATED row sequence a
// ListLogs page draws from. The endpoint has no reverse mode
// (ValidateListOptions rejects it) and paginates forward only, so rows are
// ascending by id with the cursor as an exclusive lower bound — exactly how
// the controller translates afterSequence into a LogId condition. Truncation is
// logWindowMatches' job: optional rows may or may not consume page slots, so a
// fixed prefix cut would be wrong.
func logWindowRows(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64) []logWindowRow {
	var rows []logWindowRow

	for _, row := range ls.LogRows() {
		if row.ID <= afterSeq {
			continue
		}

		match, known := matchLogFilter(ledger, row.ID, row.Date, filter)
		if known && !match {
			continue
		}

		rows = append(rows, logWindowRow{
			id: row.ID, kind: row.Kind, payload: row.Payload,
			date: row.Date, sequence: row.Sequence,
			purged: row.PurgedVolumes, newKept: row.NewKeptVolumes, ephemeral: row.EphemeralVolumes,
			required: known,
		})
	}

	return rows
}

// logWindowMatches reports whether the page is exactly a legal window over the
// candidate's row sequence: required rows appear in order, optional rows may,
// nothing else does, and a required row may only be missing past a full
// (truncated) page.
func logWindowMatches(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int, page []serverLogRow) bool {
	if len(page) > pageSize {
		return false
	}

	j := 0

	for _, row := range logWindowRows(ls, ledger, filter, afterSeq) {
		if j == len(page) {
			if len(page) == pageSize {
				return true // full page — the remaining rows were truncated
			}

			if row.required {
				return false // page had room, yet a required row is missing
			}

			continue
		}

		if page[j].id == row.id {
			if !logRowMatches(ledger, row, page[j]) {
				return false
			}

			j++
		} else if row.required {
			return false
		}
	}

	return j == len(page)
}

// logRowMatches compares a served log against the model's record of it, field
// by field. The date is compared once the model has learned it from the commit
// response; before that the row is optional and its date says nothing. Every
// other field the model derives itself, so a mismatch is the server's.
func logRowMatches(ledger string, row logWindowRow, got serverLogRow) bool {
	if got.ledger != ledger || got.kind != row.kind {
		return false
	}

	// The payload rendering is empty for a transaction log, whose content the
	// ListTransactions path validates against the model's own records.
	if row.payload != "" && got.payload != row.payload {
		return false
	}

	// The global sequence counts every ledger's logs and the technical entries
	// between them, so the model holds it only for a bulk whose response it
	// folded; before that it says nothing.
	if row.sequence != 0 && got.sequence != row.sequence {
		return false
	}

	// The three volume annotations are derived, never learned: the model runs
	// the same end-of-bulk partition, so all three are pinned exactly — an
	// empty list included.
	if got.volumesKnown &&
		(got.purged != row.purged || got.newKept != row.newKept || got.ephemeral != row.ephemeral) {
		return false
	}

	if row.date == nil {
		return true
	}

	return got.hasDate && got.date == row.date.GetData()
}

// logWindow is the page the model predicts when every row is decided: the
// required rows, truncated to pageSize. Optional rows are left out, so it is a
// diagnostic rendering only — legality is logWindowMatches, which admits them.
func logWindow(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int) []uint64 {
	var window []uint64

	for _, row := range logWindowRows(ls, ledger, filter, afterSeq) {
		if !row.required {
			continue
		}

		window = append(window, row.id)
		if len(window) == pageSize {
			break
		}
	}

	return window
}

// runLogQuery drives one ListLogs page and checks it against the model.
func runLogQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)

	var filter *commonpb.QueryFilter
	if !oneIn(4) {
		filter = genLogFilter(ledger, 0)
	}

	pageSize := queryPageSize()

	var (
		cursor   string
		afterSeq uint64
	)

	if oneIn(2) {
		afterSeq = internal.Rand().Uint64() % 16
		cursor = strconv.FormatUint(afterSeq, 10)
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	// A linearizable read is served at a fixed Raft horizon at or past every
	// bulk whose response this driver has observed, with the read projection
	// certified up to it (EN-1946), so the window stays representable by a
	// candidate base.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListLogs(readCtx, &servicepb.ListLogsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Filter:   filter,
		},
	})

	var logs []*commonpb.Log
	if err == nil {
		logs, err = drainStream(stream)
	}

	maxTicket := c.ticketSeq.Load()

	errKind, gated := classifyLogQueryError(err)
	if !gated {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListLogs returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	c.validateLogQuery(ctx, client, maxTicket, ledger, filter, afterSeq, pageSize, logs, neededLogIndexes(filter), errKind, err)
}

// classifyLogQueryError maps a ListLogs outcome to its class. ok=false means
// the error is not one the index gate produces, so the caller decides whether
// it is environmental. The not-ready rejection rides codes.Unavailable, which
// sits inside internal.IsTransient, so classifying must happen before any
// transient bail: an index gate refusing a filter the model can serve — or
// refusing one that needs no index — is a finding, not a blip.
func classifyLogQueryError(err error) (indexedErrKind, bool) {
	switch {
	case err == nil:
		return indexedErrNone, true
	case isIndexNotFound(err), isIndexNotReady(err):
		return indexedErrNotReady, true
	default:
		return indexedErrNone, false
	}
}

// serverLogIDs pulls the ledger-local ids out of a page.
func serverLogIDs(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, 0, len(logs))
	for _, l := range logs {
		out = append(out, l.GetPayload().GetApply().GetLog().GetId())
	}

	return out
}

// validateLogQuery checks one ListLogs outcome against the model. needed holds
// the canonical IDs of the indexes the filter reads (see neededLogIndexes) and
// errKind the observed outcome class, so the legal outcomes per candidate base
// are the ones every index-backed target shares (indexedQueryOutcomeLegal):
//
//   - a page is legal iff the base holds every needed index and the page is
//     that base's ordered window;
//   - a not-ready refusal is legal iff some needed index is not active on the
//     base — so a refusal of a filter needing no index is a finding, and so is
//     a page served for an index no base holds.
func (c *Checker) validateLogQuery(ctx context.Context, client servicepb.BucketServiceClient, maxTicket uint64, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int, serverLogs []*commonpb.Log, needed map[string]struct{}, errKind indexedErrKind, err error) {
	page := serverLogRows(serverLogs)
	ids := serverLogIDs(serverLogs)

	// A page must always be ascending, within the cursor, and no longer than
	// requested — properties that hold whatever the filter reads.
	for i, id := range ids {
		if id <= afterSeq || (i > 0 && id <= ids[i-1]) || len(ids) > pageSize {
			assert.Unreachable("singleton_driver_model: log page violates its own ordering", internal.Details{
				"ledger":    ledger,
				"filter":    describeFilter(filter),
				"afterSeq":  afterSeq,
				"pageSize":  pageSize,
				"serverIds": joinUint64(ids),
			})

			return
		}
	}

	// Nothing in this workload configures a signing key, so a signed log on a
	// read means the server signed something this driver cannot account for.
	for _, row := range page {
		if row.signed {
			assert.Unreachable("singleton_driver_model: log page carries a response signature", internal.Details{
				"ledger": ledger,
				"logId":  row.id,
			})

			return
		}
	}

	matched := c.matchesModel(maxTicket, "LOGQUERY", func(base oracle.GlobalState) bool {
		return logOutcomeLegal(base.Ledger(ledger), ledger, filter, needed, errKind, page, afterSeq, pageSize)
	})

	c.noteQueryCoverage(ledger, commonpb.QueryTarget_QUERY_TARGET_LOGS, filter, needed,
		matched && errKind == indexedErrNone, len(page))

	if matched {
		if errKind == indexedErrNotReady {
			assert.Reachable("singleton_driver_model: log query gated on a missing index", internal.Details{"ledger": ledger})
		} else {
			assert.Reachable("singleton_driver_model: log query served results", internal.Details{"ledger": ledger})
		}

		return
	}

	recheckIDs, recheckErr := recheckLogIDs(ctx, client, ledger)
	serverKinds, kindsErr := recheckLogKinds(ctx, client, ledger)

	details := internal.Details{
		"ledger":      ledger,
		"filter":      describeFilter(filter),
		"afterSeq":    afterSeq,
		"pageSize":    pageSize,
		"rows":        len(ids),
		"serverIds":   joinUint64(ids),
		"serverRows":  describeServerLogRows(page),
		"modelIds":    joinUint64(c.modelLogWindow(ledger, filter, afterSeq, pageSize)),
		"modelIdx":    c.describeLogIndexStates(ledger, needed),
		"modelDates":  c.describeLogDates(ledger),
		"recheck":     diagnosticDetail(joinUint64(recheckIDs), recheckErr),
		"modelKinds":  strings.Join(c.modelLogKinds(ledger), ","),
		"serverKinds": diagnosticDetail(strings.Join(serverKinds, ","), kindsErr),
	}
	if err != nil {
		details["error"] = err.Error()
	}

	assert.Unreachable("singleton_driver_model: log query outside model", details)
}

// describeServerLogRows renders every pinned field of a served page, the shape
// a field mismatch is read from: id:kind@date/seq[payload]{volume annotations}.
func describeServerLogRows(page []serverLogRow) string {
	parts := make([]string, 0, len(page))

	for _, row := range page {
		date := "?"
		if row.hasDate {
			date = strconv.FormatUint(row.date, 10)
		}

		parts = append(parts, strconv.FormatUint(row.id, 10)+":"+row.kind+"@"+date+
			"/seq"+strconv.FormatUint(row.sequence, 10)+"["+row.payload+"]"+
			"{purged="+row.purged+";newKept="+row.newKept+";ephemeral="+row.ephemeral+"}")
	}

	return strings.Join(parts, ",")
}

// logOutcomeLegal is the per-candidate verdict for one ListLogs outcome: the
// shared index-lifecycle legality, with the base's ordered log window as the
// result check.
func logOutcomeLegal(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, needed map[string]struct{}, errKind indexedErrKind, page []serverLogRow, afterSeq uint64, pageSize int) bool {
	return indexedQueryOutcomeLegal(ls, commonpb.QueryTarget_QUERY_TARGET_LOGS, filter, needed, errKind, "", func(view oracle.LedgerState) bool {
		return logWindowMatches(view, ledger, filter, afterSeq, pageSize, page)
	})
}

// describeLogDates renders the committed model's (log id, learned date) pairs,
// an unlearned date as "?" — what a date-filter finding is decided by, so it
// must be readable from the finding's details alone. Capped: a long ledger's
// tail says nothing the head does not. Acquires c.mu.
func (c *Checker) describeLogDates(ledger string) string {
	const maxRendered = 24

	c.mu.Lock()
	defer c.mu.Unlock()

	rows := c.modelState.Ledger(ledger).LogDates()

	parts := make([]string, 0, min(len(rows), maxRendered))
	for _, row := range rows {
		if len(parts) == maxRendered {
			parts = append(parts, "…")

			break
		}

		date := "?"
		if row.Date != nil {
			date = strconv.FormatUint(row.Date.GetData(), 10)
		}

		parts = append(parts, strconv.FormatUint(row.ID, 10)+"="+date)
	}

	return strings.Join(parts, ",")
}

// describeLogIndexStates renders the committed model's lifecycle state for each
// needed index, for a finding's diagnostics. Acquires c.mu.
func (c *Checker) describeLogIndexStates(ledger string, needed map[string]struct{}) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	states := make([]string, 0, len(needed))
	for canon := range needed {
		states = append(states, canon+"="+indexStateLabelFull(ls, canon))
	}
	slices.Sort(states)

	return strings.Join(states, " ")
}

// modelLogWindow returns the log window on the committed modelState for a
// finding's diagnostics. Acquires c.mu.
func (c *Checker) modelLogWindow(ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return logWindow(c.modelState.Ledger(ledger), ledger, filter, afterSeq, pageSize)
}

// equalUint64 compares two id sequences elementwise; a nil and an empty slice
// are the same empty page.
func equalUint64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// recheckLogIDs re-reads the ledger's logs unfiltered after the finding, at a
// later horizon. It separates "not yet visible at the first read's horizon"
// from "never visible": if the ids the model expected show up here, the page
// was a visibility question; if they never appear, the logs are absent.
func recheckLogIDs(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]uint64, error) {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 200},
	})
	if err != nil {
		return nil, err
	}

	logs, err := drainStream(stream)
	if err != nil {
		return nil, err
	}

	return serverLogIDs(logs), nil
}

func (c *Checker) modelLogKinds(ledger string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.modelState.Ledger(ledger).LogKinds()
}

// recheckLogKinds names the payload arm of each log the server actually holds,
// so a surplus in the model can be attributed to a request kind.
func recheckLogKinds(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]string, error) {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 200},
	})
	if err != nil {
		return nil, err
	}

	logs, err := drainStream(stream)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(logs))
	for _, l := range logs {
		switch d := l.GetPayload().GetApply().GetLog().GetData(); {
		case d.GetCreatedTransaction() != nil:
			out = append(out, "created_transaction")
		case d.GetRevertedTransaction() != nil:
			out = append(out, "reverted_transaction")
		case d.GetSavedMetadata() != nil:
			out = append(out, "saved_metadata")
		case d.GetDeletedMetadata() != nil:
			out = append(out, "deleted_metadata")
		case d.GetSetMetadataFieldType() != nil:
			out = append(out, "set_metadata_field_type")
		case d.GetRemovedMetadataFieldType() != nil:
			out = append(out, "removed_metadata_field_type")
		case d.GetCreateIndex() != nil:
			out = append(out, "create_index")
		case d.GetDropIndex() != nil:
			out = append(out, "drop_index")
		case d.GetAddedAccountType() != nil:
			out = append(out, "added_account_type")
		case d.GetRemovedAccountType() != nil:
			out = append(out, "removed_account_type")
		default:
			out = append(out, "other")
		}
	}

	return out, nil
}

// diagnosticDetail renders a best-effort recheck into a finding detail. A
// failed diagnostic RPC must be distinguishable from a genuinely empty server
// view — an empty recheck otherwise corrupts the triage of a real finding.
func diagnosticDetail(value string, err error) string {
	if err != nil {
		return "recheck failed: " + err.Error()
	}

	return value
}
