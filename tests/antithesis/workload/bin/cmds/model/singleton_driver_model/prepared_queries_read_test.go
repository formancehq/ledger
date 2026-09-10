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
