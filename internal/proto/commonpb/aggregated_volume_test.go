package commonpb

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAggregatedVolumeTotals(t *testing.T) {
	t.Parallel()

	t.Run("decodes totals above 2^256", func(t *testing.T) {
		t.Parallel()

		above := new(big.Int).Lsh(big.NewInt(3), 256)
		encoded, err := NewBigUint(above)
		require.NoError(t, err)

		input, output, err := (&AggregatedVolume{Input: encoded}).Totals()
		require.NoError(t, err)
		require.Zero(t, above.Cmp(input))
		require.Zero(t, output.Sign())
	})

	t.Run("rejects a non-canonical total", func(t *testing.T) {
		t.Parallel()

		_, _, err := (&AggregatedVolume{Output: &BigUint{Magnitude: []byte{0, 1}}}).Totals()
		require.ErrorContains(t, err, "output")
	})
}
