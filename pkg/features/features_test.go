package features

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateFeatureWithValue(t *testing.T) {
	t.Run("unknown feature", func(t *testing.T) {
		err := ValidateFeatureWithValue("DOES_NOT_EXIST", "ON")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not exists")
	})

	t.Run("valid enum feature", func(t *testing.T) {
		require.NoError(t, ValidateFeatureWithValue(FeatureMovesHistory, "ON"))
		require.NoError(t, ValidateFeatureWithValue(FeatureMovesHistory, "OFF"))
	})

	t.Run("invalid enum value", func(t *testing.T) {
		err := ValidateFeatureWithValue(FeatureMovesHistory, "INVALID")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not possible")
	})
}

func TestFeatureSet_String(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require.Equal(t, "", FeatureSet{}.String())
	})
	t.Run("single", func(t *testing.T) {
		fs := FeatureSet{FeatureMovesHistory: "ON"}
		require.Equal(t, "MH=ON", fs.String())
	})
}

func TestFeatureSet_With(t *testing.T) {
	original := FeatureSet{FeatureMovesHistory: "ON"}
	updated := original.With(FeatureMovesHistory, "OFF")
	require.Equal(t, "ON", original[FeatureMovesHistory])
	require.Equal(t, "OFF", updated[FeatureMovesHistory])
}

func TestFeatureSet_Match(t *testing.T) {
	fs := DefaultFeatures
	require.True(t, fs.Match(FeatureSet{FeatureMovesHistory: "ON"}))
	require.False(t, fs.Match(FeatureSet{FeatureMovesHistory: "OFF"}))
}

func TestShortenFeature(t *testing.T) {
	require.Equal(t, "MH", shortenFeature("MOVES_HISTORY"))
	require.Equal(t, "HL", shortenFeature("HASH_LOGS"))
}
