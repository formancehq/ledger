package main

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// The model folds buckets the way the server's result stage does: one per
// (asset, color) by default, colors summed into "" on request, precisions of
// one base merged under the highest seen with rescaling.
// A grouped aggregate carries no flat totals: every account lands in the first
// prefix that claims it, in the order the request listed them, and an account no
// prefix claims is left out.
func TestModelAggregateGroups_AssignsToTheFirstMatchingPrefix(t *testing.T) {
	t.Parallel()

	ls := buildLedger(t,
		oracletest.TxReq("world", "t-1:1", "USD", 5),
		oracletest.TxReq("world", "t-2:1", "USD", 7),
		oracletest.TxReq("world", "other:1", "USD", 9),
	)

	opts := aggOptions{groupByPrefixes: []string{"t-1:", "t-2:", "no-such-prefix:"}}
	groups := modelAggregateGroups(ls, nil, opts)

	require.Len(t, groups, 3)
	require.Equal(t, []string{"t-1:", "t-2:", "no-such-prefix:"}, []string{groups[0].prefix, groups[1].prefix, groups[2].prefix},
		"groups follow the request's order")
	require.Equal(t, "USD|=in:5,out:0", renderAgg(groups[0].sums))
	require.Equal(t, "USD|=in:7,out:0", renderAgg(groups[1].sums))
	require.Empty(t, groups[2].sums, "a prefix nothing matches still gets its entry")

	// world and other:1 match no prefix, so their cells are excluded — the
	// grouped totals are strictly less than the flat fold.
	require.Equal(t, "USD|=in:21,out:21", renderAgg(modelAggregate(ls, nil, aggOptions{})))

	// A repeated prefix is served twice, both times by the same assignment.
	repeated := modelAggregateGroups(ls, nil, aggOptions{groupByPrefixes: []string{"t-1:", "t-1:"}})
	require.Len(t, repeated, 2)
	require.Equal(t, renderAgg(repeated[0].sums), renderAgg(repeated[1].sums))
}

func TestAggGroupsEqual_ComparesPrefixOrder(t *testing.T) {
	t.Parallel()

	a := []aggGroup{{prefix: "x:"}, {prefix: "y:"}}
	require.True(t, aggGroupsEqual(a, []aggGroup{{prefix: "x:"}, {prefix: "y:"}}))
	require.False(t, aggGroupsEqual(a, []aggGroup{{prefix: "y:"}, {prefix: "x:"}}), "order is the request's")
	require.False(t, aggGroupsEqual(a, []aggGroup{{prefix: "x:"}}))
}

func TestModelAggregateOptionsMirrorTheResultStage(t *testing.T) {
	t.Parallel()

	sums := map[assetColor]*aggPair{}
	addAgg(sums, assetColor{Asset: "USD/2", Color: ""}, uint256.NewInt(100), uint256.NewInt(40))
	addAgg(sums, assetColor{Asset: "USD/2", Color: "a"}, uint256.NewInt(5), uint256.NewInt(0))
	addAgg(sums, assetColor{Asset: "USD/4", Color: ""}, uint256.NewInt(7), uint256.NewInt(7))
	addAgg(sums, assetColor{Asset: "COIN", Color: "b"}, uint256.NewInt(1), uint256.NewInt(1))

	merged := mergePrecisions(sums)
	require.Len(t, merged, 3, "USD/2 and USD/4 meet under USD/4, per color")
	require.Equal(t, "10007", merged[assetColor{Asset: "USD/4"}].in.Dec(), "USD/2 amounts scaled by 100")
	require.Equal(t, "4007", merged[assetColor{Asset: "USD/4"}].out.Dec())
	require.Equal(t, "500", merged[assetColor{Asset: "USD/4", Color: "a"}].in.Dec())
	require.Equal(t, "1", merged[assetColor{Asset: "COIN", Color: "b"}].in.Dec())

	base, precision := splitAsset("EUR/2")
	require.Equal(t, "EUR", base)
	require.Equal(t, uint8(2), precision)
	base, precision = splitAsset("COIN")
	require.Equal(t, "COIN", base)
	require.Zero(t, precision)
	require.Equal(t, "COIN", formatAsset("COIN", 0))
	require.Equal(t, "USD/4", formatAsset("USD", 4))
}

// The served result is keyed by (asset, color); a repeated bucket is refused.
func TestServerAggregateKeysByAssetAndColor(t *testing.T) {
	t.Parallel()

	vol := func(asset, color string, in, out uint64) *commonpb.AggregatedVolume {
		return &commonpb.AggregatedVolume{Asset: asset, Color: color, Input: commonpb.NewUint256(uint256.NewInt(in)), Output: commonpb.NewUint256(uint256.NewInt(out))}
	}

	got, ok := serverAggregate(&commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{vol("USD/2", "", 3, 1), vol("USD/2", "a", 2, 2), vol("EUR/2", "", 0, 0)}})
	require.True(t, ok)
	require.Len(t, got, 3, "every bucket the server sent is kept, so an invented zero bucket stays visible")
	require.Equal(t, "3", got[assetColor{Asset: "USD/2"}].in.Dec())
	require.Equal(t, "2", got[assetColor{Asset: "USD/2", Color: "a"}].out.Dec())

	_, ok = serverAggregate(&commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{vol("USD/2", "a", 1, 1), vol("USD/2", "a", 1, 1)}})
	require.False(t, ok)
}
