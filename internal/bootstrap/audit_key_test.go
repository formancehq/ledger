package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateAuditKeyDistinct(t *testing.T) {
	t.Parallel()
	a, err := generateAuditKey()
	require.NoError(t, err)
	b, err := generateAuditKey()
	require.NoError(t, err)
	require.Len(t, a, 32)
	require.Len(t, b, 32)
	require.NotEqual(t, a, b)
}
