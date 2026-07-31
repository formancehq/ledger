package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPITConvergenceFixtureUsesOwnedLedgerAndWideAmounts(t *testing.T) {
	t.Parallel()

	require.Equal(t, "pitscope-convergence", PITConvergenceLedgerName())
	require.Equal(t, []CanonicalVolume{
		{
			Asset:  "PIT-CONVERGENCE-1-PRE",
			Input:  "18446744073709551617",
			Output: "18446744073709551617",
		},
		{
			Asset:  "PIT-CONVERGENCE-2-POST",
			Input:  "340282366920938463463374607431768211457",
			Output: "340282366920938463463374607431768211457",
		},
	}, PITConvergenceExpectedVolumes())
}

func TestPITConvergenceExpectedVolumesReturnsFreshSlice(t *testing.T) {
	t.Parallel()

	first := PITConvergenceExpectedVolumes()
	first[0].Input = "mutated"

	require.Equal(t, pitConvergencePreFaultAmount, PITConvergenceExpectedVolumes()[0].Input)
}
