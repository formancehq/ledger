package ledger

import (
	"github.com/formancehq/ledger/pkg/features"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFeatures(t *testing.T) {
	f := features.MinimalFeatureSet.With(features.FeatureMovesHistory, "DISABLED")
	require.Equal(t, "DISABLED", f[features.FeatureMovesHistory])
	require.Equal(t, "AMH=DISABLED,HL=DISABLED,IAS=OFF,ITA=OFF,MH=DISABLED,MHPCEV=DISABLED,TMH=DISABLED", f.String())
}
