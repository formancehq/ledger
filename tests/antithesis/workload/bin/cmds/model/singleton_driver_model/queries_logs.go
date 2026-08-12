package main

import (
	"context"
	"strconv"

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
			LogBuiltinUint: &commonpb.LogBuiltinUintCondition{Cond: genLogUintCond()},
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

// datePredicated reports whether the filter reads the log date, which the model
// does not predict.
func datePredicated(f *commonpb.QueryFilter) bool {
	switch t := f.GetFilter().(type) {
	case nil:
		return false
	case *commonpb.QueryFilter_LogBuiltinUint:
		return true
	case *commonpb.QueryFilter_And:
		for _, sub := range t.And.GetFilters() {
			if datePredicated(sub) {
				return true
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, sub := range t.Or.GetFilters() {
			if datePredicated(sub) {
				return true
			}
		}
	case *commonpb.QueryFilter_Not:
		return datePredicated(t.Not.GetFilter())
	}

	return false
}

// matchLogFilter evaluates a date-free LOGS filter against a modelled log.
func matchLogFilter(ledger string, id uint64, f *commonpb.QueryFilter) bool {
	switch t := f.GetFilter().(type) {
	case nil:
		return true
	case *commonpb.QueryFilter_Ledger:
		return ledger == t.Ledger.GetCond().GetHardcoded()
	case *commonpb.QueryFilter_LogId:
		return matchUintCond(id, t.LogId.GetCond())
	case *commonpb.QueryFilter_And:
		for _, sub := range t.And.GetFilters() {
			if !matchLogFilter(ledger, id, sub) {
				return false
			}
		}

		return true
	case *commonpb.QueryFilter_Or:
		for _, sub := range t.Or.GetFilters() {
			if matchLogFilter(ledger, id, sub) {
				return true
			}
		}

		return false
	case *commonpb.QueryFilter_Not:
		return !matchLogFilter(ledger, id, t.Not.GetFilter())
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

	for _, id := range ls.LogIDs() {
		if id <= afterSeq {
			continue
		}

		if !matchLogFilter(ledger, id, filter) {
			continue
		}

		window = append(window, id)
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

	maxTicket := c.ticketSeq.Load()

	if err != nil {
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

	c.validateLogQuery(maxTicket, ledger, filter, afterSeq, pageSize, logs)
}

// serverLogIDs pulls the ledger-local ids out of a page.
func serverLogIDs(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, 0, len(logs))
	for _, l := range logs {
		out = append(out, l.GetPayload().GetApply().GetLog().GetId())
	}

	return out
}

func (c *Checker) validateLogQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, pageSize int, serverLogs []*commonpb.Log) {
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

	// A date leaf is not predictable — the date is server-assigned — so the
	// model can only require that the page be drawn from the logs that exist
	// and satisfy the filter's date-free part. Dropping the date leaf makes
	// the predicate strictly weaker, so every row the server may legitimately
	// return still passes; a row failing it is wrong under any date.
	if datePredicated(filter) {
		if c.matchesModel(maxTicket, "LOGQUERY", func(base oracle.GlobalState) bool {
			return logsWithinRelaxed(base.Ledger(ledger), ledger, filter, afterSeq, ids)
		}) {
			return
		}
	} else if c.matchesModel(maxTicket, "LOGQUERY", func(base oracle.GlobalState) bool {
		return equalUint64(logWindow(base.Ledger(ledger), ledger, filter, afterSeq, pageSize), ids)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: log query outside model", internal.Details{
		"ledger":    ledger,
		"filter":    describeFilter(filter),
		"afterSeq":  afterSeq,
		"pageSize":  pageSize,
		"dated":     datePredicated(filter),
		"rows":      len(ids),
		"serverIds": joinUint64(ids),
		"modelIds":  joinUint64(c.modelLogWindow(ledger, filter, afterSeq, pageSize)),
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

// logsWithinRelaxed checks a date-predicated page: every returned id must be a
// committed log of this ledger that passes the filter with its date leaves
// treated as satisfiable. It catches rows that no date could justify — an id
// the ledger never had, or one the date-free part excludes.
func logsWithinRelaxed(ls oracle.LedgerState, ledger string, filter *commonpb.QueryFilter, afterSeq uint64, ids []uint64) bool {
	known := map[uint64]bool{}
	for _, id := range ls.LogIDs() {
		known[id] = true
	}

	for _, id := range ids {
		if !known[id] || id <= afterSeq {
			return false
		}

		if !matchLogFilter(ledger, id, relaxDates(filter)) {
			return false
		}
	}

	return true
}

// relaxDates rewrites a filter so date leaves read as "true", leaving the rest
// intact. A NOT above a date leaf then reads as "false", which is the correct
// weakening: under negation an unpredictable term can exclude anything, so the
// branch asserts nothing.
func relaxDates(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	switch t := f.GetFilter().(type) {
	case nil:
		return nil
	case *commonpb.QueryFilter_LogBuiltinUint:
		return nil // nil matches everything
	case *commonpb.QueryFilter_And:
		out := make([]*commonpb.QueryFilter, 0, len(t.And.GetFilters()))
		for _, sub := range t.And.GetFilters() {
			out = append(out, relaxDates(sub))
		}

		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: out}}}
	case *commonpb.QueryFilter_Or:
		// An OR with a relaxed arm is satisfied by that arm alone, so it
		// constrains nothing.
		if datePredicated(f) {
			return nil
		}

		return f
	case *commonpb.QueryFilter_Not:
		if datePredicated(f) {
			return nil
		}

		return f
	}

	return f
}
