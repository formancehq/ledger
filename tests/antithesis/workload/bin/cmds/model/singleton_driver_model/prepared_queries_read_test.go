package main

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func u256(v uint64) *ledgerpb.Uint256 { return protohelpers.NewUint256(uint256.NewInt(v)) }

func aggVolume(asset string, in, out uint64) *ledgerpb.AggregatedVolume {
	return &ledgerpb.AggregatedVolume{Asset: asset, Input: u256(in), Output: u256(out)}
}

func aggResult(vols ...*ledgerpb.AggregatedVolume) *ledgerpb.AggregateResult {
	return &ledgerpb.AggregateResult{Volumes: vols}
}

// TestModelAggregateFoldsMatchedAccountsOnly pins the aggregate prediction: the
// sum runs over the accounts the filter selects, per asset, and an account the
// filter excludes contributes nothing.
func TestModelAggregateFoldsMatchedAccountsOnly(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "keep:1", "USD/2", 5),
		oracletest.TxReq("world", "keep:2", "USD/2", 7),
		oracletest.TxReq("world", "skip:1", "USD/2", 9),
		oracletest.TxReq("world", "keep:3", "EUR/2", 4),
	)

	got := modelAggregate(ls, filterAddrPrefix("keep:"))

	require.Equal(t, map[aggregateBucket]oracle.VolumePair{
		{asset: "USD/2"}: {Input: *uint256.NewInt(12)},
		{asset: "EUR/2"}: {Input: *uint256.NewInt(4)},
	}, got)

	// The unfiltered universe additionally picks up world's outputs and skip:1.
	all := modelAggregate(ls, nil)
	require.Equal(t, *uint256.NewInt(21), all[aggregateBucket{asset: "USD/2"}].Input)
	require.Equal(t, *uint256.NewInt(21), all[aggregateBucket{asset: "USD/2"}].Output)
}

func TestModelAggregateKeepsColorBucketsSeparate(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.TxReqColoredL("L", "world", "keep:1", "USD/2", "A", 5))

	require.Equal(t, map[aggregateBucket]oracle.VolumePair{
		{asset: "USD/2", color: "A"}: {Input: *uint256.NewInt(5)},
	}, modelAggregate(ls, filterAddrPrefix("keep:")))
}

// TestAggregateMatchesRejectsDivergence pins that the comparison is on the
// bucket SET and every total — not on sums alone. A bucket the server invents
// or drops changes the set while leaving the other totals intact, which is
// exactly the shape a volume-purge divergence takes.
func TestAggregateMatchesRejectsDivergence(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "keep:1", "USD/2", 5),
		oracletest.TxReq("world", "keep:2", "EUR/2", 3),
	)

	filter := filterAddrPrefix("keep:")
	exact := aggResult(aggVolume("USD/2", 5, 0), aggVolume("EUR/2", 3, 0))

	require.True(t, aggregateMatches(ls, filter, exact))

	// Order is not part of the contract.
	require.True(t, aggregateMatches(ls, filter, aggResult(aggVolume("EUR/2", 3, 0), aggVolume("USD/2", 5, 0))))

	for _, tc := range []struct {
		name string
		agg  *ledgerpb.AggregateResult
	}{
		{"missing bucket", aggResult(aggVolume("USD/2", 5, 0))},
		{"extra zero bucket", aggResult(aggVolume("USD/2", 5, 0), aggVolume("EUR/2", 3, 0), aggVolume("GBP/2", 0, 0))},
		{"wrong input", aggResult(aggVolume("USD/2", 6, 0), aggVolume("EUR/2", 3, 0))},
		{"wrong output", aggResult(aggVolume("USD/2", 5, 1), aggVolume("EUR/2", 3, 0))},
		{"unexpected group", &ledgerpb.AggregateResult{
			Volumes: exact.GetVolumes(),
			Groups:  []*ledgerpb.GroupedAggregateResult{{}},
		}},
		{"colored bucket the workload never produces", aggResult(
			aggVolume("USD/2", 5, 0),
			&ledgerpb.AggregatedVolume{Asset: "EUR/2", Color: "red", Input: u256(3), Output: u256(0)},
		)},
		{"empty result", aggResult()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.False(t, aggregateMatches(ls, filter, tc.agg))
		})
	}
}

// TestAggregateMatchesRejectsDuplicateBucket pins that a repeated (asset,color)
// entry is a finding rather than something the comparison folds away.
func TestAggregateMatchesRejectsDuplicateBucket(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t, oracletest.TxReq("world", "keep:1", "USD/2", 4))

	require.False(t, aggregateMatches(ls, filterAddrPrefix("keep:"),
		aggResult(aggVolume("USD/2", 2, 0), aggVolume("USD/2", 2, 0))))
}

func TestAggregateTargetRejectionUsesCandidateDefinition(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
		Name: "q", Target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	}))
	call := preparedCall{ledger: "L", name: "q", errKind: pqErrAggregateTarget}

	require.True(t, preparedAggregateOutcomeLegal(gs.Ledger("L"), call, nil),
		"a candidate where the query was recreated on a non-account target explains the rejection")
}

func TestClassifyAggregateTargetRejectionByCode(t *testing.T) {
	t.Parallel()

	target := status.New(codes.InvalidArgument, "invalid aggregate target")
	other, err := status.New(codes.NotFound, "ledger missing").WithDetails(
		&errdetails.ErrorInfo{Domain: "ledger", Reason: "LEDGER_NOT_FOUND"})
	require.NoError(t, err)

	require.Equal(t, pqErrAggregateTarget, classifyPreparedExecError(target.Err()))
	require.Equal(t, pqErrLedgerNotFound, classifyPreparedExecError(other.Err()))
}

func TestPreparedLedgerOutcomeFollowsLifecycle(t *testing.T) {
	t.Parallel()

	live := buildGlobal(t, &ledgerpb.Request{Type: &ledgerpb.Request_CreateLedger{
		CreateLedger: &ledgerpb.CreateLedgerRequest{Name: "L"},
	}}, createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
		Name: "q", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	}))
	call := preparedCall{ledger: "L", errKind: pqErrLedgerNotFound}
	handled, legal := preparedLedgerOutcomeLegal(live, call)
	require.True(t, handled)
	require.False(t, legal, "a live ledger cannot return LEDGER_NOT_FOUND")

	call.errKind = pqErrNone
	handled, legal = preparedLedgerOutcomeLegal(live, call)
	require.False(t, handled, "a live ledger must continue through prepared-query validation")
	require.False(t, legal)

	deleted := live.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{
		DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"},
	}}))
	require.True(t, deleted.OK)

	call.errKind = pqErrLedgerNotFound
	handled, legal = preparedLedgerOutcomeLegal(deleted.State, call)
	require.True(t, handled)
	require.True(t, legal, "a deleted ledger must return LEDGER_NOT_FOUND despite retained state")

	call.errKind = pqErrNone
	handled, legal = preparedLedgerOutcomeLegal(deleted.State, call)
	require.True(t, handled)
	require.False(t, legal, "a deleted ledger cannot serve its retained prepared-query snapshot")

	missing := preparedCall{ledger: "missing", errKind: pqErrLedgerNotFound}
	handled, legal = preparedLedgerOutcomeLegal(live, missing)
	require.True(t, handled)
	require.True(t, legal)

	missing.errKind = pqErrNone
	handled, legal = preparedLedgerOutcomeLegal(live, missing)
	require.True(t, handled)
	require.False(t, legal, "an absent ledger cannot serve a retained prepared-query snapshot")
}

func TestAggregateRecreationValidatesAccountsCandidateResult(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t,
		oracletest.TxReq("world", "keep:1", "USD/2", 5),
		createPreparedQueryReq("L", &ledgerpb.PreparedQuery{Name: "q", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS}),
	)
	call := preparedCall{ledger: "L", name: "q"}

	require.False(t, preparedAggregateOutcomeLegal(gs.Ledger("L"), call, aggResult()),
		"a recreated accounts query still validates aggregate contents")
	call.errKind = pqErrOther
	require.False(t, preparedAggregateOutcomeLegal(gs.Ledger("L"), call, nil),
		"an accounts candidate never explains an unexpected error")
	call.errKind = pqErrAggregateTarget
	require.False(t, preparedAggregateOutcomeLegal(gs.Ledger("L"), call, nil),
		"an accounts candidate never explains a target-validation error")
}

func TestListRejectsAggregateTargetErrorForEmptyWindow(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
		Name: "q", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		Filter: filterAddrPrefix("missing:"),
	}))
	call := preparedCall{ledger: "L", name: "q", pageSize: 10, errKind: pqErrAggregateTarget}

	require.False(t, preparedListOutcomeLegal(gs.Ledger("L"), call, nil, nil))
}

func TestPreparedOutcomesRejectWrongResponseArm(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t,
		createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
			Name: "list", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			Filter: filterAddrPrefix("missing:"),
		}),
		createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
			Name: "aggregate", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			Filter: filterAddrPrefix("missing:"),
		}),
	)

	require.False(t, preparedListOutcomeLegal(gs.Ledger("L"),
		preparedCall{ledger: "L", name: "list", pageSize: 10, wrongResult: true}, nil, nil))
	require.False(t, preparedAggregateOutcomeLegal(gs.Ledger("L"),
		preparedCall{ledger: "L", name: "aggregate", wrongResult: true}, nil))
}

func TestPreparedPaginationPreservesRawCursorOrderingAcrossTargets(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 1),
		oracletest.TxReq("world", "acc:2", "USD/2", 1),
	)
	call := preparedCall{ledger: "L", pageSize: 10}

	require.True(t, preparedWindowMatches(ls, call,
		ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil, []byte("acc:1"),
		&ledgerpb.PreparedQueryCursor{}),
		"transaction keys sort before an account-address cursor")

	require.Equal(t, []string{"acc:1", "acc:2", "world"},
		preparedAccountProbe(ls, nil, string(uint64EntityKey(1)), 10),
		"account keys sort after a big-endian transaction cursor")
}

func TestPreparedCursorMetadataEchoesRequest(t *testing.T) {
	t.Parallel()

	call := preparedCall{pageSize: 17, cursor: "opaque-cursor"}
	require.True(t, preparedCursorMetadataMatches(call, &ledgerpb.PreparedQueryCursor{
		PageSize: 17,
		Previous: "opaque-cursor",
	}))
	require.False(t, preparedCursorMetadataMatches(call, &ledgerpb.PreparedQueryCursor{
		PageSize: 16,
		Previous: "opaque-cursor",
	}))
	require.False(t, preparedCursorMetadataMatches(call, &ledgerpb.PreparedQueryCursor{
		PageSize: 17,
		Previous: "wrong",
	}))
}

func TestPreparedTransactionContinuationMustMatchRemainingRows(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 1),
		oracletest.TxReq("world", "acc:2", "USD/2", 1),
	)
	call := preparedCall{ledger: "L", pageSize: 1}
	first := []*ledgerpb.Transaction{serverTxFromRec(ls.Txs().Get(0))}

	require.False(t, preparedWindowMatches(ls, call,
		ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil, nil,
		&ledgerpb.PreparedQueryCursor{TransactionData: first}),
		"clearing has_more must not hide known remaining transactions")
	require.False(t, preparedWindowMatches(ls, call,
		ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil, nil,
		&ledgerpb.PreparedQueryCursor{TransactionData: first, HasMore: true}),
		"has_more requires a continuation cursor")
}

// TestRegistryMatches pins the listing comparison: names, targets and the
// byte-identical stored filter, as an unordered set.
func TestRegistryMatches(t *testing.T) {
	t.Parallel()

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*ledgerpb.Request{
		createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
			Name: "a", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:"),
		}),
	}})
	require.True(t, res.OK, res.Reason)

	second := res.State.Apply(oracle.Bulk{Requests: []*ledgerpb.Request{
		createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
			Name: "b", Target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, Filter: filterReference("r"),
		}),
	}})
	require.True(t, second.OK, second.Reason)

	ls := second.State.Ledger("L")

	served := []*ledgerpb.PreparedQuery{
		{Name: "b", Target: ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, Filter: filterReference("r")},
		{Name: "a", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:")},
	}
	require.True(t, registryMatches(ls, served))

	for _, tc := range []struct {
		name   string
		served []*ledgerpb.PreparedQuery
	}{
		{"missing entry", served[:1]},
		{"duplicate hides missing entry", []*ledgerpb.PreparedQuery{served[1], served[1].CloneVT()}},
		{"extra entry", append(append([]*ledgerpb.PreparedQuery{}, served...),
			&ledgerpb.PreparedQuery{Name: "c", Filter: filterAddrPrefix("y:")})},
		{"stale filter", []*ledgerpb.PreparedQuery{
			served[0],
			{Name: "a", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("stale:")},
		}},
		{"wrong target", []*ledgerpb.PreparedQuery{
			served[0],
			{Name: "a", Target: ledgerpb.QueryTarget_QUERY_TARGET_LOGS, Filter: filterAddrPrefix("x:")},
		}},
		{"renamed entry", []*ledgerpb.PreparedQuery{
			served[0],
			{Name: "z", Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:")},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.False(t, registryMatches(ls, tc.served))
		})
	}
}

func TestPreparedLogsUseActiveDateIndex(t *testing.T) {
	t.Parallel()

	filter := filterLogDateLeaf()
	gs := buildGlobal(t, oracletest.CreateIndexReq(logDateIndexID()), createPreparedQueryReq("L", &ledgerpb.PreparedQuery{
		Name: "dates", Target: ledgerpb.QueryTarget_QUERY_TARGET_LOGS, Filter: filter,
	}))
	gs.SetIndexActive("L", logDateIndexCanonical)

	call := preparedCall{ledger: "L", name: "dates", pageSize: 10}

	require.True(t, preparedListOutcomeLegal(gs.Ledger("L"), call, nil, &ledgerpb.PreparedQueryCursor{}))
	require.Equal(t, neededLogIndexes(filter), preparedNeededIndexes(filter, ledgerpb.QueryTarget_QUERY_TARGET_LOGS))

	call.errKind = pqErrIndex

	require.False(t, preparedListOutcomeLegal(gs.Ledger("L"), call, nil, nil))
}

func TestPreparedLogPageUnknownDatesAndHasMore(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5), oracletest.TxReq("world", "acc:2", "USD/2", 5))
	ls := gs.Ledger("L")
	ids := ls.LogDates()

	require.Len(t, ids, 2)

	first, second := ids[0].ID, ids[1].ID
	filter := filterLogDateLeaf()

	call := preparedCall{ledger: "L", pageSize: 1}
	page := servedRows(ls, "L", first)
	corrupt := servedRows(ls, "L", first)
	corrupt[0].kind = "wrong-kind"

	require.False(t, preparedLogWindowMatches(ls, call, filter, nil, corrupt, true))
	require.True(t, preparedLogWindowMatches(ls, call, filter, nil, page, true), "an unknown-date suffix can supply another page")
	require.True(t, preparedLogWindowMatches(ls, call, filter, nil, page, false), "the same suffix can fail the date filter")
	require.True(t, preparedLogWindowMatches(ls, call, filter, uint64EntityKey(first), servedRows(ls, "L", second), false))
	require.False(t, preparedLogWindowMatches(ls, call, filter, uint64EntityKey(first), servedRows(ls, "L", second), true), "no remaining row can justify hasMore")
	require.False(t, preparedLogWindowMatches(ls, call, filter, nil, nil, true), "hasMore requires a full page")
	require.False(t, preparedLogWindowMatches(ls, call, filter, nil, servedRows(ls, "L", 999), false))

	gs.LearnLogDate("L", second, &ledgerpb.Timestamp{Data: 5})
	ls = gs.Ledger("L")

	require.False(t, preparedLogWindowMatches(ls, call, filter, nil, page, false), "a required suffix cannot be omitted")
	require.True(t, preparedLogWindowMatches(ls, call, filter, nil, page, true))

	call.pageSize = 2

	require.False(t, preparedLogWindowMatches(ls, call, filter, nil, page, false), "a short page must include required rows")
}

func TestPreparedLogPageRejectsMissingAndInventedRows(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")

	call := preparedCall{ledger: "L", pageSize: 10}

	require.False(t, preparedLogPageMatches(ls, call, nil, nil, &ledgerpb.PreparedQueryCursor{}))

	invented := &ledgerpb.Log{Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{Apply: &ledgerpb.ApplyLedgerLog{
		LedgerName: "L", Log: &ledgerpb.LedgerLog{Id: 999},
	}}}}

	require.False(t, preparedLogPageMatches(ls, call, nil, nil, &ledgerpb.PreparedQueryCursor{LogData: []*ledgerpb.Log{invented}}))
	require.True(t, preparedLogPageMatches(ls, call, nil, uint64EntityKey(ls.LogRows()[0].ID), &ledgerpb.PreparedQueryCursor{}))
}
