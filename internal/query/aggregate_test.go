package query

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func makeEntry(ledgerName string, account, asset string, input, output uint64) attributes.ComputedEntry[*raftcmdpb.VolumePair] {
	vk := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Asset:      asset,
	}

	return attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		CanonicalKey: vk.Bytes(),
		Value: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256(uint256.NewInt(input)),
			Output: commonpb.NewUint256(uint256.NewInt(output)),
		},
	}
}

func TestVolumeAggregator_NoRescaling(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(false, false)

	require.NoError(t, va.accumulate(makeEntry("test", "a", "USD/2", 100, 50)))
	require.NoError(t, va.accumulate(makeEntry("test", "a", "USD/4", 10000, 5000)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 2)
	require.Equal(t, "USD/2", result.GetVolumes()[0].GetAsset())
	require.Equal(t, "USD/4", result.GetVolumes()[1].GetAsset())
}

func TestVolumeAggregator_UseMaxPrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true, false)

	// USD/2: 100 in, 50 out → rescaled to /4: 10000 in, 5000 out
	require.NoError(t, va.accumulate(makeEntry("test", "a", "USD/2", 100, 50)))
	// USD/4: 10000 in, 5000 out → stays as is
	require.NoError(t, va.accumulate(makeEntry("test", "b", "USD/4", 10000, 5000)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "USD/4", result.GetVolumes()[0].GetAsset())

	var gotInput, gotOutput uint256.Int
	result.GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	result.GetVolumes()[0].GetOutput().IntoUint256(&gotOutput)

	require.Equal(t, uint256.NewInt(20000), &gotInput)  // 10000 + 100*100
	require.Equal(t, uint256.NewInt(10000), &gotOutput) // 5000 + 50*100
}

func TestVolumeAggregator_UseMaxPrecision_SamePrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true, false)

	require.NoError(t, va.accumulate(makeEntry("test", "a", "EUR/2", 200, 100)))
	require.NoError(t, va.accumulate(makeEntry("test", "b", "EUR/2", 300, 150)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "EUR/2", result.GetVolumes()[0].GetAsset())

	var gotInput uint256.Int
	result.GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	require.Equal(t, uint256.NewInt(500), &gotInput)
}

func TestVolumeAggregator_UseMaxPrecision_MixedAssets(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true, false)

	require.NoError(t, va.accumulate(makeEntry("test", "a", "USD/2", 100, 0)))
	require.NoError(t, va.accumulate(makeEntry("test", "a", "USD/4", 10000, 0)))
	require.NoError(t, va.accumulate(makeEntry("test", "a", "EUR/2", 200, 0)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 2)
	// Sorted: EUR/2, USD/4
	require.Equal(t, "EUR/2", result.GetVolumes()[0].GetAsset())
	require.Equal(t, "USD/4", result.GetVolumes()[1].GetAsset())
}

func TestVolumeAggregator_UseMaxPrecision_NoPrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true, false)

	require.NoError(t, va.accumulate(makeEntry("test", "a", "GOLD", 500, 100)))
	require.NoError(t, va.accumulate(makeEntry("test", "b", "GOLD", 300, 200)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "GOLD", result.GetVolumes()[0].GetAsset())

	var gotInput uint256.Int
	result.GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	require.Equal(t, uint256.NewInt(800), &gotInput)
}

func TestGroupedAggregator_BasicPrefixes(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{
		GroupByPrefixes: []string{"users:", "merchants:"},
	})

	require.NoError(t, ga.accumulate(makeEntry("test", "users:alice", "USD/2", 100, 50)))
	require.NoError(t, ga.accumulate(makeEntry("test", "users:bob", "USD/2", 200, 100)))
	require.NoError(t, ga.accumulate(makeEntry("test", "merchants:shop1", "USD/2", 500, 250)))

	result, err := ga.result()
	require.NoError(t, err)
	require.Empty(t, result.GetVolumes(), "flat volumes should be empty for grouped result")
	require.Len(t, result.GetGroups(), 2)

	// Groups are ordered by prefix declaration order.
	require.Equal(t, "users:", result.GetGroups()[0].GetPrefix())
	require.Len(t, result.GetGroups()[0].GetVolumes(), 1)

	var usersInput uint256.Int
	result.GetGroups()[0].GetVolumes()[0].GetInput().IntoUint256(&usersInput)
	require.Equal(t, uint256.NewInt(300), &usersInput) // 100 + 200

	require.Equal(t, "merchants:", result.GetGroups()[1].GetPrefix())
	require.Len(t, result.GetGroups()[1].GetVolumes(), 1)

	var merchantsInput uint256.Int
	result.GetGroups()[1].GetVolumes()[0].GetInput().IntoUint256(&merchantsInput)
	require.Equal(t, uint256.NewInt(500), &merchantsInput)
}

func TestGroupedAggregator_UnmatchedAccountSkipped(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{
		GroupByPrefixes: []string{"users:"},
	})

	require.NoError(t, ga.accumulate(makeEntry("test", "users:alice", "USD/2", 100, 50)))
	require.NoError(t, ga.accumulate(makeEntry("test", "world", "USD/2", 9999, 9999))) // no match

	result, err := ga.result()
	require.NoError(t, err)
	require.Len(t, result.GetGroups(), 1)

	var input uint256.Int
	result.GetGroups()[0].GetVolumes()[0].GetInput().IntoUint256(&input)
	require.Equal(t, uint256.NewInt(100), &input)
}

func TestGroupedAggregator_WithMaxPrecision(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{
		UseMaxPrecision: true,
		GroupByPrefixes: []string{"users:"},
	})

	require.NoError(t, ga.accumulate(makeEntry("test", "users:alice", "USD/2", 100, 50)))
	require.NoError(t, ga.accumulate(makeEntry("test", "users:bob", "USD/4", 10000, 5000)))

	result, err := ga.result()
	require.NoError(t, err)
	require.Len(t, result.GetGroups(), 1)
	require.Len(t, result.GetGroups()[0].GetVolumes(), 1)
	require.Equal(t, "USD/4", result.GetGroups()[0].GetVolumes()[0].GetAsset())

	var gotInput uint256.Int
	result.GetGroups()[0].GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	require.Equal(t, uint256.NewInt(20000), &gotInput) // 100*100 + 10000
}

func TestGroupedAggregator_FirstPrefixWins(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{
		GroupByPrefixes: []string{"users:", "users:v"},
	})

	require.NoError(t, ga.accumulate(makeEntry("test", "users:vip1", "USD/2", 100, 50)))

	result, err := ga.result()
	require.NoError(t, err)
	// "users:vip1" matches "users:" first.
	var input uint256.Int
	result.GetGroups()[0].GetVolumes()[0].GetInput().IntoUint256(&input)
	require.Equal(t, uint256.NewInt(100), &input)

	// Second group should be empty.
	require.Empty(t, result.GetGroups()[1].GetVolumes())
}

func TestNewAccumulator_FlatByDefault(t *testing.T) {
	t.Parallel()

	acc := newAccumulator(AggregateOptions{})
	require.NoError(t, acc.accumulate(makeEntry("test", "a", "USD/2", 100, 50)))

	result, err := acc.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1)
	require.Empty(t, result.GetGroups())
}

func TestNewAccumulator_GroupedWhenPrefixes(t *testing.T) {
	t.Parallel()

	acc := newAccumulator(AggregateOptions{GroupByPrefixes: []string{"a:"}})
	require.NoError(t, acc.accumulate(makeEntry("test", "a:1", "USD/2", 100, 50)))

	result, err := acc.result()
	require.NoError(t, err)
	require.Empty(t, result.GetVolumes())
	require.Len(t, result.GetGroups(), 1)
}

func TestPow10(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint256.NewInt(1), pow10(0))
	require.Equal(t, uint256.NewInt(10), pow10(1))
	require.Equal(t, uint256.NewInt(100), pow10(2))
	require.Equal(t, uint256.NewInt(1000000), pow10(6))
}

// makeColoredEntry builds a volume entry with a non-empty color, used to
// verify the aggregator keeps color-segregated buckets distinct.
func makeColoredEntry(ledgerName, account, asset, color string, input, output uint64) attributes.ComputedEntry[*raftcmdpb.VolumePair] {
	vk := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
		Asset:      asset,
		Color:      color,
	}

	return attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		CanonicalKey: vk.Bytes(),
		Value: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256(uint256.NewInt(input)),
			Output: commonpb.NewUint256(uint256.NewInt(output)),
		},
	}
}

func TestVolumeAggregator_SegregatesColorsByDefault(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(false, false)

	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "", 100, 50)))
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "GRANTS", 200, 80)))
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "OPS", 30, 10)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 3,
		"each (asset, color) bucket must yield its own AggregatedVolume entry by default")

	byColor := map[string]*commonpb.AggregatedVolume{}
	for _, v := range result.GetVolumes() {
		byColor[v.GetColor()] = v
	}

	require.Contains(t, byColor, "")
	require.Contains(t, byColor, "GRANTS")
	require.Contains(t, byColor, "OPS")

	var got uint256.Int
	byColor["GRANTS"].GetInput().IntoUint256(&got)
	require.Equal(t, uint256.NewInt(200), &got, "GRANTS bucket must keep its own input")
}

func TestVolumeAggregator_CollapseColors(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(false, true) // collapseColors=true

	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "", 100, 50)))
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "GRANTS", 200, 80)))
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "OPS", 30, 10)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1, "collapse_colors must produce a single per-asset entry")

	vol := result.GetVolumes()[0]
	require.Equal(t, "USD/2", vol.GetAsset())
	require.Equal(t, "", vol.GetColor(), "collapsed entries are produced under the empty color")

	var input, output uint256.Int
	vol.GetInput().IntoUint256(&input)
	vol.GetOutput().IntoUint256(&output)
	require.Equal(t, uint256.NewInt(330), &input, "100+200+30")
	require.Equal(t, uint256.NewInt(140), &output, "50+80+10")
}

func TestVolumeAggregator_CollapseColors_WithMaxPrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true, true)

	// USD/2 RED: 100 in, 50 out → rescaled to /4: 10000, 5000
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/2", "RED", 100, 50)))
	// USD/4 BLUE: 1, 0
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/4", "BLUE", 1, 0)))
	// USD/4 (uncolored): 2, 1
	require.NoError(t, va.accumulate(makeColoredEntry("test", "a", "USD/4", "", 2, 1)))

	result, err := va.result()
	require.NoError(t, err)
	require.Len(t, result.GetVolumes(), 1, "collapse_colors + max_precision must collapse all to one")

	vol := result.GetVolumes()[0]
	require.Equal(t, "USD/4", vol.GetAsset())
	require.Empty(t, vol.GetColor())

	var input, output uint256.Int
	vol.GetInput().IntoUint256(&input)
	vol.GetOutput().IntoUint256(&output)
	require.Equal(t, uint256.NewInt(10003), &input, "10000 + 1 + 2")
	require.Equal(t, uint256.NewInt(5001), &output, "5000 + 0 + 1")
}

func TestGroupedAggregator_RespectsColor(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{
		GroupByPrefixes: []string{"users:"},
	})

	require.NoError(t, ga.accumulate(makeColoredEntry("test", "users:alice", "USD/2", "RED", 100, 0)))
	require.NoError(t, ga.accumulate(makeColoredEntry("test", "users:alice", "USD/2", "BLUE", 50, 0)))

	result, err := ga.result()
	require.NoError(t, err)
	require.Len(t, result.GetGroups(), 1)
	require.Len(t, result.GetGroups()[0].GetVolumes(), 2,
		"grouped aggregator must keep colors segregated within a prefix bucket")
}
