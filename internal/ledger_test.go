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

func TestGetIndexedMetadataKeys(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{}}}
		require.Nil(t, l.GetIndexedMetadataKeys())
	})
	t.Run("single key", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{
			features.FeatureIndexedMetadataKeys: "source_wallet_id",
		}}}
		require.Equal(t, []string{"source_wallet_id"}, l.GetIndexedMetadataKeys())
	})
	t.Run("multiple keys", func(t *testing.T) {
		l := Ledger{Configuration: Configuration{Features: features.FeatureSet{
			features.FeatureIndexedMetadataKeys: "source_wallet_id,destination_wallet_id",
		}}}
		require.Equal(t, []string{"source_wallet_id", "destination_wallet_id"}, l.GetIndexedMetadataKeys())
	})
}
