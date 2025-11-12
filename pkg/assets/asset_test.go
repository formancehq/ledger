package assets_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/assets"
)

func TestValidAssets(t *testing.T) {
	require.True(t, assets.IsValid("A"))
	require.True(t, assets.IsValid("USD"))
	require.True(t, assets.IsValid("USD123"))
	require.True(t, assets.IsValid("USD/2"))
	require.True(t, assets.IsValid("USD/1234"))
	require.True(t, assets.IsValid("EUR/00"))

	require.True(t, assets.IsValid("EUR_COL"))
	require.True(t, assets.IsValid("EUR_COL/12"))
}

func TestInvalidAsset(t *testing.T) {
	require.False(t, assets.IsValid(""))
	require.False(t, assets.IsValid("1"))
	require.False(t, assets.IsValid("!"))
	require.False(t, assets.IsValid("@s"))
	require.False(t, assets.IsValid("/2"))
	require.False(t, assets.IsValid("USD/"))
	require.False(t, assets.IsValid("A//2"))
	require.False(t, assets.IsValid("a"))

	require.False(t, assets.IsValid("EUR_"))
	require.False(t, assets.IsValid("_"))
	require.False(t, assets.IsValid("_C"))
	require.False(t, assets.IsValid("A_/2"))
}
