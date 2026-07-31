package internal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestPITConvergenceFixtureUsesOwnedLedgerAndWideAmounts(t *testing.T) {
	t.Parallel()

	require.Equal(t, "pitscope-convergence", PITConvergenceLedgerName())
	require.Equal(t, []CanonicalVolume{
		{
			Asset:  "PITCVGPRE",
			Input:  "18446744073709551617",
			Output: "18446744073709551617",
		},
		{
			Asset:  "PITCVGPOST",
			Input:  "340282366920938463463374607431768211457",
			Output: "340282366920938463463374607431768211457",
		},
	}, PITConvergenceExpectedVolumes())
	require.NoError(t, domain.ValidateAsset(pitConvergencePreFaultAsset))
	require.NoError(t, domain.ValidateAsset(pitConvergencePostFaultAsset))
}

func TestPITConvergenceExpectedVolumesReturnsFreshSlice(t *testing.T) {
	t.Parallel()

	first := PITConvergenceExpectedVolumes()
	first[0].Input = "mutated"

	require.Equal(t, pitConvergencePreFaultAmount, PITConvergenceExpectedVolumes()[0].Input)
}
