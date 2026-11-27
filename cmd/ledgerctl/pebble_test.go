package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPebbleCommandsRegistered(t *testing.T) {
	t.Parallel()

	root := newRootCommand()
	for _, name := range []string{"db", "find", "lsm", "manifest", "remotecat", "sstable", "wal", "blob"} {
		cmd, remaining, err := root.Find([]string{"pebble", name})
		require.NoError(t, err)
		require.Empty(t, remaining)
		require.Equal(t, "ledgerctl pebble "+name, cmd.CommandPath())
	}
}

func TestPebbleCommandIgnoresConnectionProfile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := newRootCommand()
	root.SetArgs([]string{"pebble", "db", "check", filepath.Join(t.TempDir(), "missing"), "--profile", "missing"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}
