package main

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The model folds buckets the way the server's result stage does: one per
// (asset, color) by default, colors summed into "" on request, precisions of
// one base merged under the highest seen with rescaling.
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
	require.Len(t, got, 2, "a fully-zero bucket is dropped")
	require.Equal(t, "3", got[assetColor{Asset: "USD/2"}].in.Dec())
	require.Equal(t, "2", got[assetColor{Asset: "USD/2", Color: "a"}].out.Dec())

	_, ok = serverAggregate(&commonpb.AggregateResult{Volumes: []*commonpb.AggregatedVolume{vol("USD/2", "a", 1, 1), vol("USD/2", "a", 1, 1)}})
	require.False(t, ok)
}
