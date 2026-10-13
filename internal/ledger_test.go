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

func TestNewLedger(t *testing.T) {
	t.Run("valid name", func(t *testing.T) {
		l, err := New("my-ledger", NewDefaultConfiguration())
		require.NoError(t, err)
		require.Equal(t, "my-ledger", l.Name)
		require.Equal(t, StateInitializing, l.State)
		require.Equal(t, DefaultBucket, l.Bucket)
	})

	t.Run("invalid name", func(t *testing.T) {
		_, err := New("invalid ledger!", NewDefaultConfiguration())
		require.Error(t, err)
	})

	t.Run("reserved name", func(t *testing.T) {
		_, err := New("_info", NewDefaultConfiguration())
		require.Error(t, err)
	})
}

func TestConfiguration_SetDefaults(t *testing.T) {
	cfg := Configuration{}
	cfg.SetDefaults()
	require.Equal(t, DefaultBucket, cfg.Bucket)
	require.NotNil(t, cfg.Features)
	require.Equal(t, "ON", cfg.Features[features.FeatureMovesHistory])
}

func TestLedger_HasFeature(t *testing.T) {
	l := MustNewWithDefault("test")
	require.True(t, l.HasFeature(features.FeatureMovesHistory, "ON"))
	require.False(t, l.HasFeature(features.FeatureMovesHistory, "OFF"))
}

func TestLedger_WithMetadata(t *testing.T) {
	l := MustNewWithDefault("test")
	l2 := l.WithMetadata(map[string]string{"key": "value"})
	require.Equal(t, "value", l2.Metadata["key"])
}
