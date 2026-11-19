package query

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func maxUint256() *uint256.Int {
	return new(uint256.Int).SetAllOne()
}

func makeWideEntry(account, asset, color string, input, output *uint256.Int) attributes.ComputedEntry[*raftcmdpb.VolumePair] {
	vk := domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerName: "test", Account: account},
		Asset:      asset,
		Color:      color,
	}

	return attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		CanonicalKey: vk.Bytes(),
		Value: &raftcmdpb.VolumePair{
			Input:  commonpb.NewUint256(input),
			Output: commonpb.NewUint256(output),
		},
	}
}

// bigOf returns n·(2^256−1) + extra.
func bigOf(n int64, extra int64) *big.Int {
	v := new(big.Int).Mul(maxUint256().ToBig(), big.NewInt(n))

	return v.Add(v, big.NewInt(extra))
}

func requireTotals(t *testing.T, vol *commonpb.AggregatedVolume, input, output *big.Int) {
	t.Helper()

	gotInput, gotOutput, err := vol.Totals()
	require.NoError(t, err)
	require.Zero(t, input.Cmp(gotInput), "input: want %s, got %s", input, gotInput)
	require.Zero(t, output.Cmp(gotOutput), "output: want %s, got %s", output, gotOutput)
}

// Each case sums buckets that fit individually into a total above 2^256 at a
// distinct pipeline stage.
func TestVolumeAggregator_TotalsAbove2Pow256(t *testing.T) {
	t.Parallel()

	one := uint256.NewInt(1)
	zero := new(uint256.Int)

	for _, tc := range []struct {
		name    string
		opts    AggregateOptions
		entries []attributes.ComputedEntry[*raftcmdpb.VolumePair]
		asset   string
		input   *big.Int
		output  *big.Int
	}{
		{
			name: "accumulate across accounts",
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/2", "", maxUint256(), one),
				makeWideEntry("b", "USD/2", "", maxUint256(), one),
				makeWideEntry("c", "USD/2", "", maxUint256(), zero),
			},
			asset: "USD/2", input: bigOf(3, 0), output: big.NewInt(2),
		},
		{
			name: "accumulate output",
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/2", "", one, maxUint256()),
				makeWideEntry("b", "USD/2", "", zero, maxUint256()),
			},
			asset: "USD/2", input: big.NewInt(1), output: bigOf(2, 0),
		},
		{
			name: "collapse colors",
			opts: AggregateOptions{CollapseColors: true},
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/2", "", maxUint256(), zero),
				makeWideEntry("a", "USD/2", "RED", maxUint256(), zero),
			},
			asset: "USD/2", input: bigOf(2, 0), output: big.NewInt(0),
		},
		{
			name: "max precision rescale",
			opts: AggregateOptions{UseMaxPrecision: true},
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/1", "", maxUint256(), zero),
				makeWideEntry("b", "USD/2", "", one, zero),
			},
			asset: "USD/2", input: bigOf(10, 1), output: big.NewInt(0),
		},
		{
			name: "max precision merge",
			opts: AggregateOptions{UseMaxPrecision: true},
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/1", "", zero, one),
				makeWideEntry("b", "USD/2", "", zero, maxUint256()),
			},
			asset: "USD/2", input: big.NewInt(0), output: bigOf(1, 10),
		},
		{
			name: "max precision then collapse colors",
			opts: AggregateOptions{UseMaxPrecision: true, CollapseColors: true},
			entries: []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
				makeWideEntry("a", "USD/2", "RED", maxUint256(), zero),
				makeWideEntry("a", "USD/2", "BLUE", maxUint256(), zero),
				makeWideEntry("b", "USD/1", "", one, zero),
			},
			asset: "USD/2", input: bigOf(2, 10), output: big.NewInt(0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			va := newVolumeAggregator(tc.opts)
			for _, e := range tc.entries {
				require.NoError(t, va.accumulate(e))
			}

			result, err := va.result()
			require.NoError(t, err)
			require.Len(t, result.GetVolumes(), 1)
			require.Equal(t, tc.asset, result.GetVolumes()[0].GetAsset())
			require.Empty(t, result.GetVolumes()[0].GetColor())
			requireTotals(t, result.GetVolumes()[0], tc.input, tc.output)
		})
	}
}

func TestGroupedAggregator_TotalsAbove2Pow256(t *testing.T) {
	t.Parallel()

	ga := newGroupedAggregator(AggregateOptions{GroupByPrefixes: []string{"users:", "merchants:"}})
	for _, e := range []attributes.ComputedEntry[*raftcmdpb.VolumePair]{
		makeWideEntry("users:alice", "USD/2", "", maxUint256(), uint256.NewInt(0)),
		makeWideEntry("users:bob", "USD/2", "", maxUint256(), uint256.NewInt(0)),
		makeWideEntry("merchants:shop", "USD/2", "", uint256.NewInt(4), uint256.NewInt(1)),
	} {
		require.NoError(t, ga.accumulate(e))
	}

	result, err := ga.result()
	require.NoError(t, err)
	require.Len(t, result.GetGroups(), 2)
	require.Equal(t, "users:", result.GetGroups()[0].GetPrefix())
	require.Len(t, result.GetGroups()[0].GetVolumes(), 1)
	requireTotals(t, result.GetGroups()[0].GetVolumes()[0], bigOf(2, 0), big.NewInt(0))
	require.Len(t, result.GetGroups()[1].GetVolumes(), 1)
	requireTotals(t, result.GetGroups()[1].GetVolumes()[0], big.NewInt(4), big.NewInt(1))
}

func TestWideSum(t *testing.T) {
	t.Parallel()

	var s wideSum
	for range 5 {
		s.add(maxUint256())
	}
	s.add(uint256.NewInt(9))

	require.Equal(t, uint64(5), s.wraps)
	require.Zero(t, bigOf(5, 9).Cmp(s.toBig()))
}
