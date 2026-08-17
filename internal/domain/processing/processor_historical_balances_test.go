package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessConfigureHistoricalBalances(t *testing.T) {
	t.Parallel()

	for _, enabled := range []bool{false, true} {
		payload, err := processConfigureHistoricalBalances(&raftcmdpb.ConfigureHistoricalBalancesOrder{Enabled: enabled})
		require.Nil(t, err)
		require.NotNil(t, payload.GetConfiguredHistoricalBalances())
		require.Equal(t, enabled, payload.GetConfiguredHistoricalBalances().GetEnabled())
	}
}

func TestProcessConfigureHistoricalBalancesRejectsNilOrder(t *testing.T) {
	t.Parallel()

	payload, err := processConfigureHistoricalBalances(nil)
	require.Nil(t, payload)
	require.ErrorContains(t, err, "nil ConfigureHistoricalBalancesOrder")
}
