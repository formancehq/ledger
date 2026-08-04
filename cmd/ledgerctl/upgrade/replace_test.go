package upgrade

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplaceExecutableWindows(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	currentPath := filepath.Join(dir, "ledgerctl.exe")
	replacementPath := filepath.Join(dir, "ledgerctl-new.exe")

	require.NoError(t, os.WriteFile(currentPath, []byte("old"), 0o755))
	require.NoError(t, os.WriteFile(replacementPath, []byte("new"), 0o755))
	require.NoError(t, replaceExecutable(currentPath, replacementPath, "windows"))

	current, err := os.ReadFile(currentPath)
	require.NoError(t, err)
	require.Equal(t, "new", string(current))

	backup, err := os.ReadFile(currentPath + ".old")
	require.NoError(t, err)
	require.Equal(t, "old", string(backup))
}

func TestReplaceExecutableWindowsRemovesPreviousBackup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	currentPath := filepath.Join(dir, "ledgerctl.exe")
	replacementPath := filepath.Join(dir, "ledgerctl-new.exe")

	require.NoError(t, os.WriteFile(currentPath, []byte("current"), 0o755))
	require.NoError(t, os.WriteFile(currentPath+".old", []byte("previous backup"), 0o755))
	require.NoError(t, os.WriteFile(replacementPath, []byte("replacement"), 0o755))
	require.NoError(t, replaceExecutable(currentPath, replacementPath, "windows"))

	backup, err := os.ReadFile(currentPath + ".old")
	require.NoError(t, err)
	require.Equal(t, "current", string(backup))
}

func TestReplaceExecutableWindowsRollsBack(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	currentPath := filepath.Join(dir, "ledgerctl.exe")
	replacementPath := filepath.Join(dir, "missing.exe")

	require.NoError(t, os.WriteFile(currentPath, []byte("current"), 0o755))
	require.Error(t, replaceExecutable(currentPath, replacementPath, "windows"))

	current, err := os.ReadFile(currentPath)
	require.NoError(t, err)
	require.Equal(t, "current", string(current))
	_, err = os.Stat(currentPath + ".old")
	require.ErrorIs(t, err, os.ErrNotExist)
}
