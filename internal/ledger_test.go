package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/features"
)

func TestFeatures(t *testing.T) {
	f := features.MinimalFeatureSet.With(features.FeatureMovesHistory, "DISABLED")
	require.Equal(t, "DISABLED", f[features.FeatureMovesHistory])
	require.Equal(t, "AMH=DISABLED,HL=DISABLED,MH=DISABLED,MHPCEV=DISABLED,TMH=DISABLED", f.String())
}
