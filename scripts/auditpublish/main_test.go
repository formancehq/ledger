package main

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublishUsesAnchoredWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	originalDirectory := filepath.Join(root, "output")
	movedDirectory := filepath.Join(root, "moved-output")
	replacementDirectory := filepath.Join(root, "replacement")
	require.NoError(t, os.Mkdir(originalDirectory, 0o700))
	require.NoError(t, os.Mkdir(replacementDirectory, 0o700))

	sourcePath := filepath.Join(root, "result.json")
	require.NoError(t, os.WriteFile(sourcePath, []byte(`{"head":"validated"}`), 0o600))
	protectedPath := filepath.Join(replacementDirectory, "report.json")
	require.NoError(t, os.WriteFile(protectedPath, []byte("tracked"), 0o600))

	t.Chdir(originalDirectory)
	require.NoError(t, os.Rename(originalDirectory, movedDirectory))
	require.NoError(t, os.Symlink(replacementDirectory, originalDirectory))

	require.NoError(t, publish(sourcePath, "report.json"))
	require.FileExists(t, filepath.Join(movedDirectory, "report.json"))
	require.Equal(t, "tracked", string(requireFile(t, protectedPath)))
}

func TestPublishReplacesHardLinkWithoutMutatingTarget(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hard links require platform-specific privileges on Windows")
	}

	root := t.TempDir()
	sourcePath := filepath.Join(root, "result.json")
	protectedPath := filepath.Join(root, "tracked")
	require.NoError(t, os.WriteFile(sourcePath, []byte("validated"), 0o600))
	require.NoError(t, os.WriteFile(protectedPath, []byte("tracked"), 0o600))
	require.NoError(t, os.Link(protectedPath, filepath.Join(root, "report.json")))

	t.Chdir(root)

	require.NoError(t, publish(sourcePath, "report.json"))
	require.Equal(t, "tracked", string(requireFile(t, protectedPath)))
	require.Equal(t, "validated", string(requireFile(t, filepath.Join(root, "report.json"))))
}

func TestPublishRejectsDestinationSymlink(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "result.json")
	protectedPath := filepath.Join(root, "tracked")
	require.NoError(t, os.WriteFile(sourcePath, []byte("validated"), 0o600))
	require.NoError(t, os.WriteFile(protectedPath, []byte("tracked"), 0o600))
	require.NoError(t, os.Symlink(protectedPath, filepath.Join(root, "report.json")))

	t.Chdir(root)

	require.ErrorContains(t, publish(sourcePath, "report.json"), "must not be a symlink")
	require.Equal(t, "tracked", string(requireFile(t, protectedPath)))
}

func requireFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)

	return contents
}
