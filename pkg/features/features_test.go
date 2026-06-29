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

	t.Run("indexed metadata keys empty", func(t *testing.T) {
		require.NoError(t, ValidateFeatureWithValue(FeatureIndexedMetadataKeys, ""))
	})

	t.Run("indexed metadata keys single", func(t *testing.T) {
		require.NoError(t, ValidateFeatureWithValue(FeatureIndexedMetadataKeys, "source_wallet_id"))
	})

	t.Run("indexed metadata keys multiple", func(t *testing.T) {
		require.NoError(t, ValidateFeatureWithValue(FeatureIndexedMetadataKeys, "source_wallet_id,destination_wallet_id"))
	})

	t.Run("indexed metadata keys invalid chars", func(t *testing.T) {
		err := ValidateFeatureWithValue(FeatureIndexedMetadataKeys, "key-with-dash")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("indexed metadata keys SQL injection attempt", func(t *testing.T) {
		err := ValidateFeatureWithValue(FeatureIndexedMetadataKeys, "key'; DROP TABLE--")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})
}
