//go:build !windows

package upgrade

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplaceExecutableUnix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	currentPath := filepath.Join(dir, "ledgerctl")
	replacementPath := filepath.Join(dir, "ledgerctl-new")

	require.NoError(t, os.WriteFile(currentPath, []byte("old"), 0o755))
	require.NoError(t, os.WriteFile(replacementPath, []byte("new"), 0o755))
	require.NoError(t, replaceExecutable(currentPath, replacementPath, "linux"))

	current, err := os.ReadFile(currentPath)
	require.NoError(t, err)
	require.Equal(t, "new", string(current))
	_, err = os.Stat(currentPath + ".old")
	require.ErrorIs(t, err, os.ErrNotExist)
}
