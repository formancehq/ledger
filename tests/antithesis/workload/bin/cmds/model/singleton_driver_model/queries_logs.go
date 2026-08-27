package main

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"google.golang.org/grpc/metadata"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// LOGS is the only target whose universe comes from the read index rather than
// the main store, so it is the one shape that is always owed cross-store
// alignment (query.AlignmentOwed). Nothing else in the driver reaches it.
//
// Two of the three LOGS-valid leaves are predictable from the model: the
// ledger name and the ledger-local log id. The third, the log date, is
// server-assigned, so it is generated but never predicted — a filter carrying
// one drops to the weaker check in validateLogQuery.

// genLogFilter builds a filter over the conditions valid on LOGS: ledger name,
// log id range, and the boolean combinators. Returns nil for the unfiltered
// case, which exercises the universe scan itself.
func genLogFilter(ledger string, depth int) *commonpb.QueryFilter {
	if depth >= 2 || oneIn(3) {
		return genLogLeaf(ledger)
	}

	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
				genLogFilter(ledger, depth+1), genLogFilter(ledger, depth+1),
			}},
		}}
	case 1:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
				genLogFilter(ledger, depth+1), genLogFilter(ledger, depth+1),
			}},
		}}
	default:
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: genLogFilter(ledger, depth+1)},
		}}
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
	switch t := f.GetFilter().(type) {
	case *commonpb.QueryFilter_LogBuiltinUint:
		return true
	case *commonpb.QueryFilter_And:
		for _, sub := range t.And.GetFilters() {
			if hasDateLeaf(sub) {
				return true
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, sub := range t.Or.GetFilters() {
			if hasDateLeaf(sub) {
				return true
			}
		}
	case *commonpb.QueryFilter_Not:
		return hasDateLeaf(t.Not.GetFilter())
	}

	return false
}

// matchLogFilter evaluates a LOGS filter against one modelled log. date is the
// server-assigned value learned at commit; it is nil only for a log whose
// response has not been folded yet, and a date leaf then matches nothing —
// the caller must not compare such a window (logWindow skips those rows).
func matchLogFilter(ledger string, id uint64, date *commonpb.Timestamp, f *commonpb.QueryFilter) bool {
	switch t := f.GetFilter().(type) {
	case nil:
		return true
	case *commonpb.QueryFilter_Ledger:
		return ledger == t.Ledger.GetCond().GetHardcoded()
	case *commonpb.QueryFilter_LogId:
		return matchUintCond(id, t.LogId.GetCond())
	case *commonpb.QueryFilter_LogBuiltinUint:
		return date != nil && matchUintCond(date.GetData(), t.LogBuiltinUint.GetCond())
	case *commonpb.QueryFilter_And:
		for _, sub := range t.And.GetFilters() {
			if !matchLogFilter(ledger, id, date, sub) {
				return false
			}
		}

		return true
	case *commonpb.QueryFilter_Or:
		for _, sub := range t.Or.GetFilters() {
			if matchLogFilter(ledger, id, date, sub) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Not:
		return !matchLogFilter(ledger, id, date, t.Not.GetFilter())
	}

	// Every LOGS-valid condition is handled above; anything else means the
	// generator and the matcher have drifted apart.
	panic("model: unmatched LOGS condition")
}

func matchUintCond(v uint64, c *commonpb.UintCondition) bool {
	if c.Min != nil {
		if v < c.GetMin() || (c.GetMinExclusive() && v == c.GetMin()) {
			return false
		}
	}

	if c.Max != nil {
		if v > c.GetMax() || (c.GetMaxExclusive() && v == c.GetMax()) {
			return false
		}
	}

	return true
}

// logWindow is the model's prediction of a ListLogs page. The endpoint has no
// reverse mode (ValidateListOptions rejects it) and paginates forward only, so
// the window is ascending by id with the cursor applied as an exclusive lower
// bound — exactly how the controller translates afterSequence into a LogId
// condition.
func logWindow(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int) []uint64 {
	var window []uint64

	for _, row := range ls.LogDates() {
		if row.ID <= afterSeq {
			continue
		}

		if !matchLogFilter(ledger, row.ID, row.Date, filter) {
			continue
		}

		window = append(window, row.ID)
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
	minSeq := c.observedFrontier()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListLogs(readCtx, &servicepb.ListLogsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var logs []*commonpb.Log
	if err == nil {
		logs, err = drainStream(stream)
	}

	diag := streamDiag(stream)

	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		// The log date is served by an opt-in builtin index this driver never
		// creates, so a filter reading it is expected to be refused. That
		// refusal is the coverage: it exercises the index gate on LOGS.
		if hasDateLeaf(filter) && status.Code(err) == codes.FailedPrecondition {
			return
		}

		assert.Unreachable("singleton_driver_model: ListLogs returned unexpected error", internal.Details{
			"ledger":     ledger,
			"filter":     describeFilter(filter),
			"error":      err.Error(),
			"serverDiag": diag,
		})

		return
	}

	c.validateLogQuery(ctx, client, maxTicket, ledger, filter, afterSeq, pageSize, logs, diag)
}

// serverLogIDs pulls the ledger-local ids out of a page.
func serverLogIDs(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, 0, len(logs))
	for _, l := range logs {
		out = append(out, l.GetPayload().GetApply().GetLog().GetId())
	}

	return out
}

func (c *Checker) validateLogQuery(ctx context.Context, client servicepb.BucketServiceClient, maxTicket uint64, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int, serverLogs []*commonpb.Log, diag ...string) {
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

	if c.matchesModel(maxTicket, "LOGQUERY", func(base oracle.GlobalState) bool {
		return equalUint64(logWindow(base.Ledger(ledger), ledger, filter, afterSeq, pageSize), ids)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: log query outside model", internal.Details{
		"serverDiag":  firstDiag(diag),
		"ledger":      ledger,
		"filter":      describeFilter(filter),
		"afterSeq":    afterSeq,
		"pageSize":    pageSize,
		"rows":        len(ids),
		"serverIds":   joinUint64(ids),
		"modelIds":    joinUint64(c.modelLogWindow(ledger, filter, afterSeq, pageSize)),
		"recheck":     joinUint64(recheckLogIDs(ctx, client, ledger)),
		"modelKinds":  strings.Join(c.modelLogKinds(ledger), ","),
		"serverKinds": strings.Join(recheckLogKinds(ctx, client, ledger), ","),
	})
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

// recheckLogIDs re-reads the ledger's logs unfiltered and unpinned, after the
// finding. It separates "not yet visible at the read's pin" from "never
// visible": if the ids the model expected show up here, the page was a
// visibility question; if they never appear, the logs are absent.
func recheckLogIDs(ctx context.Context, client servicepb.BucketServiceClient, ledger string) []uint64 {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 200},
	})
	if err != nil {
		return nil
	}

	logs, err := drainStream(stream)
	if err != nil {
		return nil
	}

	return serverLogIDs(logs)
}

func (c *Checker) modelLogKinds(ledger string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.modelState.Ledger(ledger).LogKinds()
}

// recheckLogKinds names the payload arm of each log the server actually holds,
// so a surplus in the model can be attributed to a request kind.
func recheckLogKinds(ctx context.Context, client servicepb.BucketServiceClient, ledger string) []string {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledger,
		Options: &commonpb.ListOptions{PageSize: 200},
	})
	if err != nil {
		return nil
	}

	logs, err := drainStream(stream)
	if err != nil {
		return nil
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

	return out
}
