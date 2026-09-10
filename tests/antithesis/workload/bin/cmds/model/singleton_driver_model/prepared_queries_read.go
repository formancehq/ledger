package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The two prepared-query reads: ListPreparedQueries over the registry, and
// ExecutePreparedQuery over a stored definition.
//
// Both validate against candidate bases, and both read the DEFINITION from the
// base rather than from a driver-side copy. That is the whole trick: a
// concurrent update rewrites the filter, a concurrent delete removes it, and
// each candidate base carries its own answer — so a page computed under the
// base whose registry the server actually served is accepted, and one that
// matches no base is a finding.

// --- ListPreparedQueries -------------------------------------------------

// runListPreparedQueries reads a ledger's whole registry and checks it against
// the model. The RPC has no pagination and no ordering contract, so the check
// is a set comparison on (name, target, filter).
func runListPreparedQueries(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	resp, err := client.ListPreparedQueries(readCtx, &servicepb.ListPreparedQueriesRequest{Ledger: ledger})

	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListPreparedQueries returned unexpected error", internal.Details{
			"ledger": ledger,
			"error":  err.Error(),
		})

		return
	}

	c.validateListPreparedQueries(maxTicket, ledger, resp.GetQueries())
}

// validateListPreparedQueries checks a registry listing against the model:
// legal iff some candidate base holds exactly these names, each with the same
// target and the byte-identical stored filter.
func (c *Checker) validateListPreparedQueries(maxTicket uint64, ledger string, served []*commonpb.PreparedQuery) {
	if c.matchesModel(maxTicket, "PQLIST", func(base oracle.GlobalState) bool {
		return registryMatches(base.Ledger(ledger), served)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: prepared query listing outside model", internal.Details{
		"ledger":       ledger,
		"rows":         len(served),
		"serverQuery":  describePreparedQueries(served),
		"modelQueries": c.modelPreparedQueries(ledger),
	})
}

// registryMatches compares a served listing against one base's registry. The
// listing is unordered by contract, so it is compared as a keyed set; the
// filter comparison is exact (EqualVT), which is what makes a stale or
// half-applied update visible.
func registryMatches(ls oracle.LedgerState, served []*commonpb.PreparedQuery) bool {
	if len(served) != ls.PreparedQueries().Len() {
		return false
	}

	for _, q := range served {
		stored, ok := ls.PreparedQuery(q.GetName())
		if !ok || stored.GetTarget() != q.GetTarget() || !stored.GetFilter().EqualVT(q.GetFilter()) {
			return false
		}
	}

	return true
}

// modelPreparedQueries renders the committed registry for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) modelPreparedQueries(ledger string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)

	out := make([]*commonpb.PreparedQuery, 0, ls.PreparedQueries().Len())
	for _, name := range ls.PreparedQueryNames() {
		q, _ := ls.PreparedQuery(name)
		out = append(out, q)
	}

	return describePreparedQueries(out)
}

// --- ExecutePreparedQuery ------------------------------------------------

// pqErrKind buckets an ExecutePreparedQuery failure. Everything but pqErrOther
// is a documented outcome the model can predict; pqErrOther is a finding unless
// the call was the deliberate target-misuse probe.
type pqErrKind int

const (
	pqErrNone pqErrKind = iota
	pqErrNotFound
	pqErrIndex
	pqErrCompilation
	pqErrOther
)

// classifyPreparedExecError buckets err by its error reason — the same stable
// surface classifyIndexedQueryError uses — never by message text.
func classifyPreparedExecError(err error) pqErrKind {
	switch {
	case err == nil:
		return pqErrNone
	case internal.HasErrorReason(err, "PREPARED_QUERY_NOT_FOUND"):
		return pqErrNotFound
	case isIndexNotFound(err), isIndexNotReady(err):
		return pqErrIndex
	case internal.HasErrorReason(err, "FILTER_COMPILATION_ERROR"):
		return pqErrCompilation
	default:
		return pqErrOther
	}
}

// asIndexedErrKind projects a prepared-query failure onto the indexed-query
// vocabulary, so indexedQueryOutcomeLegal — which already encodes the index
// lifecycle and retype-window rules — can be reused verbatim.
func asIndexedErrKind(kind pqErrKind) indexedErrKind {
	switch kind {
	case pqErrIndex:
		return indexedErrNotReady
	case pqErrCompilation:
		return indexedErrCompilation
	default:
		return indexedErrNone
	}
}

// runExecutePreparedQuery drives one ExecutePreparedQuery call and checks it
// against the model. The stored definition is snapshotted only to SHAPE the
// call (which parameters to bind, which mode is applicable); validation reads
// the definition from each candidate base.
func runExecutePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)
	name := preparedQueryName()

	snapshot, ls := c.preparedQuerySnapshot(ledger, name)

	params, complete := genPreparedParams(ls, snapshot.GetFilter())

	// AGGREGATE_VOLUMES is only valid on an ACCOUNTS-target query. Rolled on a
	// non-ACCOUNTS one it becomes the misuse probe: the server must reject it.
	mode := commonpb.QueryMode_QUERY_MODE_LIST
	if snapshot != nil && oneIn(3) {
		mode = commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
	}

	if mode == commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES &&
		snapshot.GetTarget() != commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		runAggregateTargetMisuse(ctx, client, c, ledger, name, params)

		return
	}

	pageSize := queryPageSize()

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	resp, err := client.ExecutePreparedQuery(readCtx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:     ledger,
		QueryName:  name,
		Parameters: params,
		PageSize:   uint32(pageSize),
		Mode:       mode,
	})

	maxTicket := c.ticketSeq.Load()

	if err != nil && (internal.IsTransient(err) && !isIndexNotReady(err) || isShutdownError(err)) {
		return
	}

	call := preparedCall{
		ledger:   ledger,
		name:     name,
		params:   params,
		complete: complete,
		pageSize: pageSize,
		errKind:  classifyPreparedExecError(err),
		err:      err,
	}

	if !complete {
		// Coverage trace for the deliberately spoiled binding: the rejection is
		// what the negative path exists to observe, so record that it happened
		// rather than inferring it from the absence of a finding.
		dbg("PQPARAM spoiled errKind=%d", int(call.errKind))
	}

	if mode == commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES {
		c.validateExecuteAggregate(maxTicket, call, resp.GetAggregate())

		return
	}

	cursor := resp.GetCursor()
	c.validateExecuteList(maxTicket, call, "", cursor)

	// One follow-on page, driven by the server's own cursor: the model's
	// after-key is the last row of the page just validated, so the opaque
	// cursor is never decoded.
	if call.errKind != pqErrNone || cursor.GetNext() == "" {
		return
	}

	c.runExecuteNextPage(ctx, client, call, cursor)
}

// preparedCall carries one ExecutePreparedQuery invocation's inputs and outcome
// through validation, keeping the validators' signatures readable.
type preparedCall struct {
	ledger   string
	name     string
	params   preparedParams
	complete bool
	pageSize int
	errKind  pqErrKind
	err      error
}

// runExecuteNextPage issues the follow-on page for prev and validates it with
// the after-key derived from prev's last row.
func (c *Checker) runExecuteNextPage(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	call preparedCall,
	prev *commonpb.PreparedQueryCursor,
) {
	after := lastPageKey(prev)
	if after == "" {
		return
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	resp, err := client.ExecutePreparedQuery(readCtx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:     call.ledger,
		QueryName:  call.name,
		Parameters: call.params,
		PageSize:   uint32(call.pageSize),
		Cursor:     prev.GetNext(),
		Mode:       commonpb.QueryMode_QUERY_MODE_LIST,
	})

	maxTicket := c.ticketSeq.Load()

	if err != nil && (internal.IsTransient(err) && !isIndexNotReady(err) || isShutdownError(err)) {
		return
	}

	call.errKind = classifyPreparedExecError(err)
	call.err = err

	c.validateExecuteList(maxTicket, call, after, resp.GetCursor())
}

// preparedQuerySnapshot returns the committed stored definition for (ledger,
// name) — nil when absent — alongside the ledger state the parameter generator
// draws plausible values from. Shaping only: validation re-reads the definition
// from each candidate base. Acquires c.mu.
func (c *Checker) preparedQuerySnapshot(ledger, name string) (*commonpb.PreparedQuery, oracle.LedgerState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)
	q, _ := ls.PreparedQuery(name)

	return q, ls
}

// lastPageKey renders the model-side after-key of a page: the last account
// address, transaction id, or log id it returned. Empty when the page is empty
// — there is nothing to page past.
func lastPageKey(cur *commonpb.PreparedQueryCursor) string {
	if accts := cur.GetAccountData(); len(accts) > 0 {
		return accts[len(accts)-1].GetAddress()
	}

	if txs := cur.GetTransactionData(); len(txs) > 0 {
		return strconv.FormatUint(txs[len(txs)-1].GetId(), 10)
	}

	if ids := serverLogIDs(cur.GetLogData()); len(ids) > 0 {
		return strconv.FormatUint(ids[len(ids)-1], 10)
	}

	return ""
}

// runAggregateTargetMisuse issues AGGREGATE_VOLUMES against a query whose
// stored target is not ACCOUNTS. The executor rejects that combination
// outright, so the only assertion is that it did reject: the rejection carries
// no error reason, and matching its message text would pin the driver to a
// string the server is free to reword.
func runAggregateTargetMisuse(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	c *Checker,
	ledger, name string,
	params preparedParams,
) {
	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()

	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	resp, err := client.ExecutePreparedQuery(readCtx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:     ledger,
		QueryName:  name,
		Parameters: params,
		Mode:       commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
	})

	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		dbg("PQAGGMISUSE rejected")
		assert.Reachable("singleton_driver_model: aggregate on a non-accounts prepared query rejected", internal.Details{
			"ledger": ledger,
			"query":  name,
		})

		return
	}

	// A success is legal only if every candidate base disagrees with the
	// snapshot that shaped the call — the query was concurrently recreated on
	// the ACCOUNTS target between the snapshot and the call.
	if c.matchesModel(maxTicket, "PQAGGMISUSE", func(base oracle.GlobalState) bool {
		stored, ok := base.Ledger(ledger).PreparedQuery(name)

		return ok && stored.GetTarget() == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: aggregate accepted on a non-accounts prepared query", internal.Details{
		"ledger":  ledger,
		"query":   name,
		"volumes": describeAggregate(resp.GetAggregate()),
	})
}

// --- LIST validation -----------------------------------------------------

// validateExecuteList checks one ExecutePreparedQuery LIST page. A page is
// legal iff SOME candidate base explains the whole outcome: the query's
// presence, its stored filter bound with the supplied parameters, the index
// lifecycle verdict, and the ordered window itself — all on the same base.
//
// after is the model-side cursor: "" for a first page, else the previous
// page's last key (address, transaction id, or log id).
func (c *Checker) validateExecuteList(maxTicket uint64, call preparedCall, after string, cur *commonpb.PreparedQueryCursor) {
	if call.errKind == pqErrOther {
		assert.Unreachable("singleton_driver_model: prepared query execution returned unexpected error", internal.Details{
			"ledger": call.ledger,
			"query":  call.name,
			"params": describeParams(call.params),
			"error":  call.err.Error(),
		})

		return
	}

	if c.matchesModel(maxTicket, "PQEXEC", func(base oracle.GlobalState) bool {
		return preparedListOutcomeLegal(base.Ledger(call.ledger), call, after, cur)
	}) {
		return
	}

	serverRows, modelRows := c.preparedPageDiag(call, after, cur)

	assert.Unreachable("singleton_driver_model: prepared query page outside model", internal.Details{
		"ledger":       call.ledger,
		"query":        call.name,
		"params":       describeParams(call.params),
		"paramsFull":   call.complete,
		"after":        after,
		"pageSize":     call.pageSize,
		"errKind":      int(call.errKind),
		"error":        errorDetail(call.err),
		"indexes":      c.preparedIndexDiag(call),
		"hasMore":      cur.GetHasMore(),
		"rows":         len(cur.GetAccountData()) + len(cur.GetTransactionData()) + len(cur.GetLogData()),
		"serverRows":   serverRows,
		"modelRows":    modelRows,
		"modelQueries": c.modelPreparedQueries(call.ledger),
	})
}

// preparedListOutcomeLegal is the whole per-base verdict for a LIST page.
func preparedListOutcomeLegal(ls oracle.LedgerState, call preparedCall, after string, cur *commonpb.PreparedQueryCursor) bool {
	stored, exists := ls.PreparedQuery(call.name)
	if !exists {
		// The only outcome an absent query can produce.
		return call.errKind == pqErrNotFound
	}

	if call.errKind == pqErrNotFound {
		return false
	}

	bound, resolved := substituteParams(stored.GetFilter(), call.params)
	if !resolved {
		return unresolvedParamRejectionLegal(ls, bound, stored.GetTarget(), call.errKind)
	}

	return indexedQueryOutcomeLegal(ls, stored.GetTarget(), bound, preparedNeededIndexes(bound, stored.GetTarget()),
		asIndexedErrKind(call.errKind), rejectedIndexLabel(call.err),
		func(view oracle.LedgerState) bool {
			return preparedWindowMatches(view, call, stored.GetTarget(), bound, after, cur)
		})
}

// unresolvedParamRejectionLegal is the verdict for a base whose stored filter
// references a parameter the call did not supply, or supplied with the wrong
// value type. Such a base can only have produced a rejection — but not
// necessarily the parameter's own one: the compiler resolves an index before it
// resolves that leaf's parameter, so a filter that ALSO needs an index the base
// lacks legitimately fails on the index first. Both rejections are therefore
// legal; results never are.
func unresolvedParamRejectionLegal(
	ls oracle.LedgerState,
	bound *commonpb.QueryFilter,
	target commonpb.QueryTarget,
	errKind pqErrKind,
) bool {
	switch errKind {
	case pqErrCompilation:
		return true
	case pqErrIndex:
		for canon := range preparedNeededIndexes(bound, target) {
			if exists, active := ls.IndexState(canon); !exists || !active {
				return true
			}
		}

		return false
	default:
		return false
	}
}

// preparedNeededIndexes names the indexes a bound filter needs on its target.
//
// LOGS is classified here rather than by neededIndexCanonicals: every log
// condition falls through that function's default arm to the never-built
// sentinel, which is right for an accounts/transactions filter (a log leaf
// there really is unservable) but wrong on the LOGS target, where the ledger
// and log-id leaves are served straight off the log stream with no read-store
// index. The one genuine need is the log-date builtin, an opt-in index this
// driver never creates — so a stored filter reading the date is permanently
// refused, exactly as the ad-hoc log query path expects (hasDateLeaf +
// isIndexNotFound in runLogQuery). Mapping it onto the never-built sentinel
// makes that rejection always legal and results never legal, with no separate
// branch in the verdict.
func preparedNeededIndexes(bound *commonpb.QueryFilter, target commonpb.QueryTarget) map[string]struct{} {
	needed := map[string]struct{}{}

	if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
		if hasDateLeaf(bound) {
			needed[neverBuiltIndexCanonical] = struct{}{}
		}

		return needed
	}

	neededIndexCanonicals(bound, target, needed)

	return needed
}

// txAscending is the value transactionWindowRows wants for an ASCENDING window.
// Its flag is inverted (`descending := !reverse`) because the ad-hoc
// ListTransactions surface defaults to newest-first, and `reverse` flips it to
// oldest-first. Prepared execution has no reverse option: executeList calls
// readstore.PaginateForward over the entity keyspace, which walks the
// big-endian transaction ids upwards. So the prepared path is the ad-hoc
// surface's REVERSED order, and passing the intuitive `false` here predicts a
// descending page the server never returns.
const txAscending = true

// preparedWindowMatches compares one page against the window the base predicts.
// Execution is forward-only (PaginateForward), so there is no reverse case.
func preparedWindowMatches(
	ls oracle.LedgerState,
	call preparedCall,
	target commonpb.QueryTarget,
	bound *commonpb.QueryFilter,
	after string,
	cur *commonpb.PreparedQueryCursor,
) bool {
	switch target {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return preparedAccountPageMatches(ls, call, bound, after, cur)
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		afterID, ok := parseAfterUint(after)
		if !ok {
			return false
		}

		return len(cur.GetAccountData()) == 0 && len(cur.GetLogData()) == 0 &&
			txWindowMatches(ls, bound, afterID, call.pageSize, txAscending, cur.GetTransactionData())
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		afterSeq, ok := parseAfterUint(after)
		if !ok {
			return false
		}

		return len(cur.GetAccountData()) == 0 && len(cur.GetTransactionData()) == 0 &&
			preparedLogPageMatches(ls, call, bound, afterSeq, cur)
	default:
		return false
	}
}

// preparedAccountPageMatches checks the rows, their content, and has_more. The
// account window is exact, so has_more is checkable: it is set iff a further
// row exists past the page.
func preparedAccountPageMatches(
	ls oracle.LedgerState,
	call preparedCall,
	bound *commonpb.QueryFilter,
	after string,
	cur *commonpb.PreparedQueryCursor,
) bool {
	if len(cur.GetTransactionData()) > 0 || len(cur.GetLogData()) > 0 {
		return false
	}

	serverAccts := cur.GetAccountData()

	// One row past the page tells us whether the server was right to advertise
	// more, without a second window computation.
	probe := preparedAccountProbe(ls, bound, after, call.pageSize+1)

	want := probe
	if len(want) > call.pageSize {
		want = want[:call.pageSize]
	}

	if len(want) != len(serverAccts) || cur.GetHasMore() != (len(probe) > call.pageSize) {
		return false
	}

	for i, addr := range want {
		if serverAccts[i].GetAddress() != addr || !accountMatches(ls, addr, serverAccts[i]) {
			return false
		}
	}

	return true
}

// preparedLogPageMatches checks the log ids and has_more; log rows carry
// server-assigned dates, so the row content itself is not predicted (the same
// limit validateLogQuery works under).
func preparedLogPageMatches(
	ls oracle.LedgerState,
	call preparedCall,
	bound *commonpb.QueryFilter,
	afterSeq uint64,
	cur *commonpb.PreparedQueryCursor,
) bool {
	ids := serverLogIDs(cur.GetLogData())

	probe := logWindow(ls, call.ledger, bound, afterSeq, call.pageSize+1)

	want := probe
	if len(want) > call.pageSize {
		want = want[:call.pageSize]
	}

	return equalUint64(want, ids) && cur.GetHasMore() == (len(probe) > call.pageSize)
}

// parseAfterUint reads the model-side cursor for the id-keyed targets; "" means
// a first page, which starts after id 0.
func parseAfterUint(after string) (uint64, bool) {
	if after == "" {
		return 0, true
	}

	v, err := strconv.ParseUint(after, 10, 64)
	if err != nil {
		return 0, false
	}

	return v, true
}

// --- AGGREGATE_VOLUMES validation ----------------------------------------

// validateExecuteAggregate checks an AGGREGATE_VOLUMES result. The aggregate is
// the flat sum of every matching account's volume cells, one bucket per
// (asset, color) — ExecutePreparedQuery exposes no grouping, max-precision or
// colour-collapse option, so the executor calls AggregateVolumes with the
// zero-valued AggregateOptions.
//
// Bucket PRESENCE is asserted, not just the sums: a zero-valued cell the server
// purged but the model kept (or the reverse) changes the bucket set while
// leaving every total identical, and comparing sums alone would miss it.
func (c *Checker) validateExecuteAggregate(maxTicket uint64, call preparedCall, agg *commonpb.AggregateResult) {
	if call.errKind == pqErrOther {
		assert.Unreachable("singleton_driver_model: prepared query aggregate returned unexpected error", internal.Details{
			"ledger": call.ledger,
			"query":  call.name,
			"params": describeParams(call.params),
			"error":  call.err.Error(),
		})

		return
	}

	if c.matchesModel(maxTicket, "PQAGG", func(base oracle.GlobalState) bool {
		return preparedAggregateOutcomeLegal(base.Ledger(call.ledger), call, agg)
	}) {
		return
	}

	assert.Unreachable("singleton_driver_model: prepared query aggregate outside model", internal.Details{
		"ledger":        call.ledger,
		"query":         call.name,
		"params":        describeParams(call.params),
		"errKind":       int(call.errKind),
		"error":         errorDetail(call.err),
		"indexes":       c.preparedIndexDiag(call),
		"serverVolumes": describeAggregate(agg),
		"modelQueries":  c.modelPreparedQueries(call.ledger),
	})
}

func preparedAggregateOutcomeLegal(ls oracle.LedgerState, call preparedCall, agg *commonpb.AggregateResult) bool {
	stored, exists := ls.PreparedQuery(call.name)
	if !exists {
		return call.errKind == pqErrNotFound
	}

	if call.errKind == pqErrNotFound {
		return false
	}

	// The call was shaped for an ACCOUNTS-target query; a base holding another
	// target explains only the rejection, which runAggregateTargetMisuse owns.
	if stored.GetTarget() != commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return false
	}

	bound, resolved := substituteParams(stored.GetFilter(), call.params)
	if !resolved {
		return unresolvedParamRejectionLegal(ls, bound, stored.GetTarget(), call.errKind)
	}

	return indexedQueryOutcomeLegal(ls, stored.GetTarget(), bound, preparedNeededIndexes(bound, stored.GetTarget()),
		asIndexedErrKind(call.errKind), rejectedIndexLabel(call.err),
		func(view oracle.LedgerState) bool {
			return aggregateMatches(view, bound, agg)
		})
}

// aggregateBucket is one (asset, color) total. The workload emits no colored
// postings, so the model only ever produces the "" bucket — a colored bucket in
// a server result is therefore a divergence, which comparing keyed sets catches.
type aggregateBucket struct {
	asset string
	color string
}

// aggregateMatches folds the base's volume cells over the accounts the filter
// selects and compares the bucket set and every total exactly.
func aggregateMatches(ls oracle.LedgerState, bound *commonpb.QueryFilter, agg *commonpb.AggregateResult) bool {
	want := modelAggregate(ls, bound)

	got := map[aggregateBucket]oracle.VolumePair{}
	for _, v := range agg.GetVolumes() {
		key := aggregateBucket{asset: v.GetAsset(), color: v.GetColor()}
		if _, dup := got[key]; dup {
			// One entry per bucket: a repeated bucket is a server-side fold bug
			// that summing into a map would silently absorb.
			return false
		}

		var pair oracle.VolumePair
		v.GetInput().IntoUint256(&pair.Input)
		v.GetOutput().IntoUint256(&pair.Output)
		got[key] = pair
	}

	if len(got) != len(want) {
		return false
	}

	for key, wantPair := range want {
		gotPair, ok := got[key]
		if !ok || !gotPair.Input.Eq(&wantPair.Input) || !gotPair.Output.Eq(&wantPair.Output) {
			return false
		}
	}

	return true
}

// modelAggregate is the model's prediction: every volume cell of every account
// the filter selects, summed per (asset, color). Grouping is by the account
// universe the ACCOUNTS compiler iterates — the same universe accountWindow
// pages over — so the aggregate and the list agree on membership by
// construction.
func modelAggregate(ls oracle.LedgerState, bound *commonpb.QueryFilter) map[aggregateBucket]oracle.VolumePair {
	matched := map[string]struct{}{}

	if base, precision, bare := hasAssetTarget(bound); bare {
		// Same universe rule as preparedAccountProbe. A purged account carries
		// no volume cell, so it adds nothing to the totals — but taking the
		// universe from the projection the server iterates keeps the aggregate
		// and the list agreeing on membership by construction rather than by
		// coincidence.
		for _, addr := range ls.EverAssetAccounts(base, precision) {
			matched[addr] = struct{}{}
		}
	} else {
		for _, addr := range accountUniverse(ls) {
			if matchAccountFilter(ls, bound, addr) {
				matched[addr] = struct{}{}
			}
		}
	}

	out := map[aggregateBucket]oracle.VolumePair{}

	for key, pair := range ls.Volumes().All() {
		if _, ok := matched[key.Address]; !ok {
			continue
		}

		bucket := aggregateBucket{asset: key.Asset}

		acc := out[bucket]
		acc.Input.Add(&acc.Input, &pair.Input)
		acc.Output.Add(&acc.Output, &pair.Output)
		out[bucket] = acc
	}

	return out
}

// describeAggregate renders an aggregate result for a finding's details,
// bucket-sorted so model and server renderings line up.
func describeAggregate(agg *commonpb.AggregateResult) string {
	parts := make([]string, 0, len(agg.GetVolumes()))
	for _, v := range agg.GetVolumes() {
		var in, out uint256.Int
		v.GetInput().IntoUint256(&in)
		v.GetOutput().IntoUint256(&out)

		parts = append(parts, fmt.Sprintf("%s/%s=%s:%s", v.GetAsset(), v.GetColor(), in.Dec(), out.Dec()))
	}

	sort.Strings(parts)

	return strings.Join(parts, " ")
}

// preparedPageDiag renders the page the server returned and the page the
// COMMITTED model state predicts for it, so a finding names the divergence
// instead of only reporting that one exists. Acquires c.mu.
func (c *Checker) preparedPageDiag(call preparedCall, after string, cur *commonpb.PreparedQueryCursor) (string, string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(call.ledger)

	stored, exists := ls.PreparedQuery(call.name)
	if !exists {
		return preparedServerRows(cur), "<query absent from committed state>"
	}

	bound, resolved := substituteParams(stored.GetFilter(), call.params)
	if !resolved {
		return preparedServerRows(cur), "<parameters unresolved against committed filter>"
	}

	switch stored.GetTarget() {
	case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
		return preparedServerRows(cur), strings.Join(preparedAccountProbe(ls, bound, after, call.pageSize), ",")
	case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
		afterID, _ := parseAfterUint(after)

		return preparedServerRows(cur), joinUint64(transactionWindow(ls, bound, afterID, call.pageSize, txAscending))
	case commonpb.QueryTarget_QUERY_TARGET_LOGS:
		afterSeq, _ := parseAfterUint(after)

		return preparedServerRows(cur), joinUint64(logWindow(ls, call.ledger, bound, afterSeq, call.pageSize))
	default:
		return preparedServerRows(cur), "<non-executable target>"
	}
}

// preparedServerRows renders a page's keys, whichever target it carries.
func preparedServerRows(cur *commonpb.PreparedQueryCursor) string {
	if accts := cur.GetAccountData(); len(accts) > 0 {
		addrs := make([]string, len(accts))
		for i, a := range accts {
			addrs[i] = a.GetAddress()
		}

		return strings.Join(addrs, ",")
	}

	if txs := cur.GetTransactionData(); len(txs) > 0 {
		ids := make([]uint64, len(txs))
		for i, tx := range txs {
			ids[i] = tx.GetId()
		}

		return joinUint64(ids)
	}

	if ids := serverLogIDs(cur.GetLogData()); len(ids) > 0 {
		return joinUint64(ids)
	}

	return ""
}

// errorDetail renders an observed error for a finding's details.
func errorDetail(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// preparedIndexDiag names the indexes the committed stored filter needs and the
// state the model holds each one in — the pair a not-ready verdict turns on.
// Acquires c.mu.
func (c *Checker) preparedIndexDiag(call preparedCall) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(call.ledger)

	stored, exists := ls.PreparedQuery(call.name)
	if !exists {
		return "<query absent from committed state>"
	}

	bound, resolved := substituteParams(stored.GetFilter(), call.params)
	if !resolved {
		return "<parameters unresolved against committed filter>"
	}

	needed := preparedNeededIndexes(bound, stored.GetTarget())
	if len(needed) == 0 {
		return "<none needed>"
	}

	canons := make([]string, 0, len(needed))
	for canon := range needed {
		canons = append(canons, canon+"="+indexStateLabelFull(ls, canon))
	}

	sort.Strings(canons)

	return strings.Join(canons, " ")
}

// preparedAccountProbe is the ordered account window for a bound ACCOUNTS
// filter, one row longer than the page so has_more is checkable.
//
// A bare has-asset leaf takes its own universe. The has-asset index serves the
// EVER-touched projection — an account drained to zero and purged from the
// volume table stays in it — while accountUniverse holds only accounts with a
// live volume or metadata cell. Paging a has-asset query over accountUniverse
// silently drops every purged account the server correctly returns.
// genAccountAssetFilter never composes the leaf with anything else for the same
// reason, and requireBareHasAsset holds the stored filters to that.
func preparedAccountProbe(ls oracle.LedgerState, bound *commonpb.QueryFilter, after string, limit int) []string {
	if base, precision, bare := hasAssetTarget(bound); bare {
		return assetWindow(ls, base, precision, after, limit, false)
	}

	return accountWindow(ls, bound, after, limit, false)
}
