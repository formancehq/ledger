package ledger

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFeatures(t *testing.T) {
	f := MinimalFeatureSet.With(FeatureMovesHistory, "DISABLED")
	require.Equal(t, "DISABLED", f[FeatureMovesHistory])
	require.Equal(t, "AMH=DISABLED,HL=DISABLED,IAS=OFF,ITA=OFF,MH=DISABLED,MHPCEV=DISABLED,TMH=DISABLED", f.String())
}
