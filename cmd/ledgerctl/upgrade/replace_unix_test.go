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

func TestCopyReplacementPreservesMode(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "downloaded-ledgerctl")
	dstPath := filepath.Join(dir, "staged-ledgerctl")

	require.NoError(t, os.WriteFile(srcPath, []byte("new binary"), 0o600))
	require.NoError(t, os.WriteFile(dstPath, nil, 0o600))
	require.NoError(t, copyReplacement(srcPath, dstPath, 0o755))

	contents, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, "new binary", string(contents))

	info, err := os.Stat(dstPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}
