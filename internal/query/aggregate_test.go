package query

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func makeEntry(ledger, account, asset string, input, output uint64) attributes.ComputedEntry[*raftcmdpb.VolumePair] {
	vk := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
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

	va := newVolumeAggregator(false)

	require.NoError(t, va.accumulate(makeEntry("l", "a", "USD/2", 100, 50)))
	require.NoError(t, va.accumulate(makeEntry("l", "a", "USD/4", 10000, 5000)))

	result := va.result()
	require.Len(t, result.GetVolumes(), 2)
	require.Equal(t, "USD/2", result.GetVolumes()[0].GetAsset())
	require.Equal(t, "USD/4", result.GetVolumes()[1].GetAsset())
}

func TestVolumeAggregator_UseMaxPrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true)

	// USD/2: 100 in, 50 out → rescaled to /4: 10000 in, 5000 out
	require.NoError(t, va.accumulate(makeEntry("l", "a", "USD/2", 100, 50)))
	// USD/4: 10000 in, 5000 out → stays as is
	require.NoError(t, va.accumulate(makeEntry("l", "b", "USD/4", 10000, 5000)))

	result := va.result()
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

	va := newVolumeAggregator(true)

	require.NoError(t, va.accumulate(makeEntry("l", "a", "EUR/2", 200, 100)))
	require.NoError(t, va.accumulate(makeEntry("l", "b", "EUR/2", 300, 150)))

	result := va.result()
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "EUR/2", result.GetVolumes()[0].GetAsset())

	var gotInput uint256.Int
	result.GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	require.Equal(t, uint256.NewInt(500), &gotInput)
}

func TestVolumeAggregator_UseMaxPrecision_MixedAssets(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true)

	require.NoError(t, va.accumulate(makeEntry("l", "a", "USD/2", 100, 0)))
	require.NoError(t, va.accumulate(makeEntry("l", "a", "USD/4", 10000, 0)))
	require.NoError(t, va.accumulate(makeEntry("l", "a", "EUR/2", 200, 0)))

	result := va.result()
	require.Len(t, result.GetVolumes(), 2)
	// Sorted: EUR/2, USD/4
	require.Equal(t, "EUR/2", result.GetVolumes()[0].GetAsset())
	require.Equal(t, "USD/4", result.GetVolumes()[1].GetAsset())
}

func TestVolumeAggregator_UseMaxPrecision_NoPrecision(t *testing.T) {
	t.Parallel()

	va := newVolumeAggregator(true)

	require.NoError(t, va.accumulate(makeEntry("l", "a", "GOLD", 500, 100)))
	require.NoError(t, va.accumulate(makeEntry("l", "b", "GOLD", 300, 200)))

	result := va.result()
	require.Len(t, result.GetVolumes(), 1)
	require.Equal(t, "GOLD", result.GetVolumes()[0].GetAsset())

	var gotInput uint256.Int
	result.GetVolumes()[0].GetInput().IntoUint256(&gotInput)
	require.Equal(t, uint256.NewInt(800), &gotInput)
}

func TestPow10(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint256.NewInt(1), pow10(0))
	require.Equal(t, uint256.NewInt(10), pow10(1))
	require.Equal(t, uint256.NewInt(100), pow10(2))
	require.Equal(t, uint256.NewInt(1000000), pow10(6))
}
