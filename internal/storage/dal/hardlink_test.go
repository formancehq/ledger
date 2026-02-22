package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHardLink_SimpleDirectory(t *testing.T) {
	t.Parallel()

	src := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(src, "file1.txt"), []byte("hello"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(src, "file2.txt"), []byte("world"), 0644))

	dst := filepath.Join(t.TempDir(), "linked")

	err := HardLink(src, dst)
	require.NoError(t, err)

	// Verify files exist in destination
	data, err := os.ReadFile(filepath.Join(dst, "file1.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	data, err = os.ReadFile(filepath.Join(dst, "file2.txt"))
	require.NoError(t, err)
	require.Equal(t, "world", string(data))
}

func TestHardLink_NestedDirectories(t *testing.T) {
	t.Parallel()

	src := t.TempDir()
	sub := filepath.Join(src, "sub")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(src, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("nested"), 0644))

	dst := filepath.Join(t.TempDir(), "linked")

	err := HardLink(src, dst)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(dst, "root.txt"))
	require.NoError(t, err)
	require.Equal(t, "root", string(data))

	data, err = os.ReadFile(filepath.Join(dst, "sub", "nested.txt"))
	require.NoError(t, err)
	require.Equal(t, "nested", string(data))
}

func TestHardLink_DstAlreadyExists(t *testing.T) {
	t.Parallel()

	src := t.TempDir()
	dst := t.TempDir() // Already exists

	err := HardLink(src, dst)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dstDir already exists")
}

func TestHardLink_SrcNotDirectory(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "file.txt")
	require.NoError(t, os.WriteFile(src, []byte("not a dir"), 0644))

	dst := filepath.Join(t.TempDir(), "linked")

	err := HardLink(src, dst)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a directory")
}

func TestHardLink_SrcNotExist(t *testing.T) {
	t.Parallel()

	dst := filepath.Join(t.TempDir(), "linked")

	err := HardLink("/nonexistent/path", dst)
	require.Error(t, err)
}
