package assets_test

import (
	"testing"

	"github.com/formancehq/ledger/pkg/assets"
	"github.com/stretchr/testify/require"
)

func TestValidAssets(t *testing.T) {
	require.True(t, assets.IsValid("A"))
	require.True(t, assets.IsValid("USD"))
	require.True(t, assets.IsValid("USD/2"))
	require.True(t, assets.IsValid("USD/1234"))
	require.True(t, assets.IsValid("EUR/00"))
}

func TestInvalidAsset(t *testing.T) {
	require.False(t, assets.IsValid(""))
	require.False(t, assets.IsValid("!"))
	require.False(t, assets.IsValid("@s"))
	require.False(t, assets.IsValid("/2"))
	require.False(t, assets.IsValid("USD/"))
	require.False(t, assets.IsValid("A//2"))
	require.False(t, assets.IsValid("a"))
}
