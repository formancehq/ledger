package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

const logTestPageSize = 8

func filterLogDateLeaf() *commonpb.QueryFilter {
	min := uint64(1)

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
		LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
			Cond:  &commonpb.UintCondition{Min: &min},
		},
	}}
}

func filterLogIDLeaf() *commonpb.QueryFilter {
	min := uint64(1)

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
		LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{Min: &min}},
	}}
}

func reasonErr(t *testing.T, code codes.Code, reason string) error {
	t.Helper()

	st, err := status.New(code, reason).WithDetails(&errdetails.ErrorInfo{Domain: "ledger", Reason: reason})
	require.NoError(t, err)

	return st.Err()
}

// The date leaf is the only LOGS leaf served from an index, wherever it sits in
// the tree; every other shape needs none.
func TestNeededLogIndexes(t *testing.T) {
	t.Parallel()

	require.Equal(t, map[string]struct{}{logDateIndexCanonical: {}}, neededLogIndexes(filterLogDateLeaf()))
	require.Equal(t, map[string]struct{}{logDateIndexCanonical: {}},
		neededLogIndexes(filterOr(filterLogIDLeaf(), filterLogDateLeaf())), "a nested date leaf still needs the index")
	require.Nil(t, neededLogIndexes(filterLogIDLeaf()))
	require.Nil(t, neededLogIndexes(nil), "the unfiltered universe scan needs no index")
}

// Both index-gate rejections must reach the validator. INDEX_BUILDING rides
// codes.Unavailable and therefore sits inside internal.IsTransient: classifying
// it before the transient bail is what keeps a wrongly gated read a finding
// instead of a skipped blip.
func TestClassifyLogQueryError(t *testing.T) {
	t.Parallel()

	kind, gated := classifyLogQueryError(nil)
	require.True(t, gated)
	require.Equal(t, indexedErrNone, kind)

	notFound := reasonErr(t, codes.FailedPrecondition, "INDEX_NOT_FOUND")
	kind, gated = classifyLogQueryError(notFound)
	require.True(t, gated)
	require.Equal(t, indexedErrNotReady, kind)

	building := reasonErr(t, codes.Unavailable, "INDEX_BUILDING")
	require.True(t, internal.IsTransient(building), "the trap: INDEX_BUILDING is inside the transient set")

	kind, gated = classifyLogQueryError(building)
	require.True(t, gated, "an index-building refusal must reach the validator, not the transient bail")
	require.Equal(t, indexedErrNotReady, kind)

	_, gated = classifyLogQueryError(status.Error(codes.Unavailable, "connection refused"))
	require.False(t, gated, "a plain Unavailable stays environmental")

	_, gated = classifyLogQueryError(status.Error(codes.FailedPrecondition, "boom"))
	require.False(t, gated, "a reasonless precondition error is not an index-gate outcome")
}

// With the log-date index absent from the model, the refusal is REQUIRED, not
// merely tolerated: a page is a finding even when its rows are exactly the
// predicted window, which is what a server that stopped gating date filters
// would return.
func TestLogOutcome_AbsentDateIndexRequiresTheRefusal(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogDateLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, window, 0, logTestPageSize),
		"a page served from an index no base holds is a finding, matching rows or not")
	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"the not-ready refusal is the legal outcome while the index is absent")
}

// Once the model holds the index active the obligation flips: the page is owed,
// its rows are checked, and a refusal is a finding.
func TestLogOutcome_ActiveDateIndexRequiresThePage(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5), oracletest.CreateIndexReq(logDateIndexID()))
	gs.SetIndexActive("L", logDateIndexCanonical)

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, window, 0, logTestPageSize))
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, append(window, 999), 0, logTestPageSize),
		"a page whose rows are not the base's window is a finding")
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"refusing an index every base holds active is a finding")
}

// A log whose date this base has not learned cannot be decided against a date
// leaf — the shape a candidate base holds after folding an observed-but-
// undrained bulk — so the page may carry it or not, but nothing else.
func TestLogWindowMatches_UnlearnedDateIsOptional(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogDateLeaf()

	rows := ls.LogDates()
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Date, "the fixture must leave the date unlearned")

	id := rows[0].ID

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, nil),
		"an undecided row may be absent from the page")
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, []uint64{id}),
		"and it may be present")
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, []uint64{id + 7}),
		"a row the base does not hold at all is never legal")
}

// Once the date is learned the row is decided: a matching log is required, so a
// page with room that omits it is a finding.
func TestLogWindowMatches_LearnedDateIsRequired(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5))

	id := gs.Ledger("L").LogDates()[0].ID
	gs.LearnLogDate("L", id, &commonpb.Timestamp{Data: 5})

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf() // date >= 1

	require.Equal(t, []uint64{id}, logWindow(ls, "L", filter, 0, logTestPageSize))
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, []uint64{id}))
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, nil),
		"a page with room may not omit a required row")
}

// A page may be truncated but never gap-toothed: carrying a later required row
// while skipping an earlier one is a finding, which is what a server serving
// the wrong version's keyspace would return.
func TestLogWindowMatches_RequiredRowMayNotBeSkipped(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "USD/2", 7),
	)

	rows := gs.Ledger("L").LogDates()
	require.Len(t, rows, 2)

	for _, row := range rows {
		gs.LearnLogDate("L", row.ID, &commonpb.Timestamp{Data: 5})
	}

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf() // date >= 1: both rows required

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, []uint64{rows[0].ID, rows[1].ID}))
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, []uint64{rows[1].ID}),
		"skipping an earlier required row is a finding")
	require.True(t, logWindowMatches(ls, "L", filter, 0, 1, []uint64{rows[0].ID}),
		"a full page of one legitimately truncates the rest")
	require.False(t, logWindowMatches(ls, "L", filter, 0, 1, []uint64{rows[1].ID}),
		"a full page must still start at the first required row")
}

// The generator churns the log-date index, so the LOGS date filters cycle
// through absent, ambiguous and active instead of only ever being refused.
func TestWorkloadIndexes_IncludesTheLogDateIndex(t *testing.T) {
	t.Parallel()

	var found bool
	for _, wi := range workloadIndexes() {
		if wi.canonical == logDateIndexCanonical {
			found = true
		}
	}

	require.True(t, found, "the log-date index must be in the create/drop pool")
}

// A filter needing no index may never be gated.
func TestLogOutcome_IndexFreeFilterMayNotBeGated(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogIDLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.NotEmpty(t, window, "the id filter must select the committed log, or this pins nothing")
	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, window, 0, logTestPageSize))
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, nil, 0, logTestPageSize),
		"an empty page where the model holds rows is a finding")
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"a not-ready refusal of an index-free filter is a finding")
}
