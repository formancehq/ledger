package main

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func u256(v uint64) *commonpb.Uint256 { return commonpb.NewUint256(uint256.NewInt(v)) }

func aggVolume(asset string, in, out uint64) *commonpb.AggregatedVolume {
	return &commonpb.AggregatedVolume{Asset: asset, Input: u256(in), Output: u256(out)}
}

func aggResult(vols ...*commonpb.AggregatedVolume) *commonpb.AggregateResult {
	return &commonpb.AggregateResult{Volumes: vols}
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
		agg  *commonpb.AggregateResult
	}{
		{"missing bucket", aggResult(aggVolume("USD/2", 5, 0))},
		{"extra zero bucket", aggResult(aggVolume("USD/2", 5, 0), aggVolume("EUR/2", 3, 0), aggVolume("GBP/2", 0, 0))},
		{"wrong input", aggResult(aggVolume("USD/2", 6, 0), aggVolume("EUR/2", 3, 0))},
		{"wrong output", aggResult(aggVolume("USD/2", 5, 1), aggVolume("EUR/2", 3, 0))},
		{"colored bucket the workload never produces", aggResult(
			aggVolume("USD/2", 5, 0),
			&commonpb.AggregatedVolume{Asset: "EUR/2", Color: "red", Input: u256(3), Output: u256(0)},
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

// TestRegistryMatches pins the listing comparison: names, targets and the
// byte-identical stored filter, as an unordered set.
func TestRegistryMatches(t *testing.T) {
	t.Parallel()

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{
		createPreparedQueryReq("L", &commonpb.PreparedQuery{
			Name: "a", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:"),
		}),
	}})
	require.True(t, res.OK, res.Reason)

	second := res.State.Apply(oracle.Bulk{Requests: []*servicepb.Request{
		createPreparedQueryReq("L", &commonpb.PreparedQuery{
			Name: "b", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, Filter: filterReference("r"),
		}),
	}})
	require.True(t, second.OK, second.Reason)

	ls := second.State.Ledger("L")

	served := []*commonpb.PreparedQuery{
		{Name: "b", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, Filter: filterReference("r")},
		{Name: "a", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:")},
	}
	require.True(t, registryMatches(ls, served))

	for _, tc := range []struct {
		name   string
		served []*commonpb.PreparedQuery
	}{
		{"missing entry", served[:1]},
		{"duplicate hides missing entry", []*commonpb.PreparedQuery{served[1], served[1].CloneVT()}},
		{"extra entry", append(append([]*commonpb.PreparedQuery{}, served...),
			&commonpb.PreparedQuery{Name: "c", Filter: filterAddrPrefix("y:")})},
		{"stale filter", []*commonpb.PreparedQuery{
			served[0],
			{Name: "a", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("stale:")},
		}},
		{"wrong target", []*commonpb.PreparedQuery{
			served[0],
			{Name: "a", Target: commonpb.QueryTarget_QUERY_TARGET_LOGS, Filter: filterAddrPrefix("x:")},
		}},
		{"renamed entry", []*commonpb.PreparedQuery{
			served[0],
			{Name: "z", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, Filter: filterAddrPrefix("x:")},
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
	gs := buildGlobal(t, oracletest.CreateIndexReq(logDateIndexID()), createPreparedQueryReq("L", &commonpb.PreparedQuery{
		Name: "dates", Target: commonpb.QueryTarget_QUERY_TARGET_LOGS, Filter: filter,
	}))
	gs.SetIndexActive("L", logDateIndexCanonical)

	call := preparedCall{ledger: "L", name: "dates", pageSize: 10}

	require.True(t, preparedListOutcomeLegal(gs.Ledger("L"), call, "", &commonpb.PreparedQueryCursor{}))
	require.Equal(t, neededLogIndexes(filter), preparedNeededIndexes(filter, commonpb.QueryTarget_QUERY_TARGET_LOGS))

	call.errKind = pqErrIndex

	require.False(t, preparedListOutcomeLegal(gs.Ledger("L"), call, "", nil))
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

	require.False(t, preparedLogWindowMatches(ls, call, filter, 0, corrupt, true))
	require.True(t, preparedLogWindowMatches(ls, call, filter, 0, page, true), "an unknown-date suffix can supply another page")
	require.True(t, preparedLogWindowMatches(ls, call, filter, 0, page, false), "the same suffix can fail the date filter")
	require.True(t, preparedLogWindowMatches(ls, call, filter, first, servedRows(ls, "L", second), false))
	require.False(t, preparedLogWindowMatches(ls, call, filter, first, servedRows(ls, "L", second), true), "no remaining row can justify hasMore")
	require.False(t, preparedLogWindowMatches(ls, call, filter, 0, nil, true), "hasMore requires a full page")
	require.False(t, preparedLogWindowMatches(ls, call, filter, 0, servedRows(ls, "L", 999), false))

	gs.LearnLogDate("L", second, &commonpb.Timestamp{Data: 5})
	ls = gs.Ledger("L")

	require.False(t, preparedLogWindowMatches(ls, call, filter, 0, page, false), "a required suffix cannot be omitted")
	require.True(t, preparedLogWindowMatches(ls, call, filter, 0, page, true))

	call.pageSize = 2

	require.False(t, preparedLogWindowMatches(ls, call, filter, 0, page, false), "a short page must include required rows")
}

func TestPreparedLogPageRejectsMissingAndInventedRows(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")

	call := preparedCall{ledger: "L", pageSize: 10}

	require.False(t, preparedLogPageMatches(ls, call, nil, 0, &commonpb.PreparedQueryCursor{}))

	invented := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "L", Log: &commonpb.LedgerLog{Id: 999},
	}}}}

	require.False(t, preparedLogPageMatches(ls, call, nil, 0, &commonpb.PreparedQueryCursor{LogData: []*commonpb.Log{invented}}))
	require.True(t, preparedLogPageMatches(ls, call, nil, ls.LogRows()[0].ID, &commonpb.PreparedQueryCursor{}))
}
