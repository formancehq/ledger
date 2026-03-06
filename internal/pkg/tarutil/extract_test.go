package tarutil

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func createTarArchive(t *testing.T, entries []tarEntry) io.Reader {
	t.Helper()

	var buf bytes.Buffer

	tw := tar.NewWriter(&buf)

	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.name,
			Mode:     e.mode,
			Typeflag: e.typeflag,
			Size:     int64(len(e.content)),
		}
		require.NoError(t, tw.WriteHeader(hdr))

		if len(e.content) > 0 {
			_, err := tw.Write([]byte(e.content))
			require.NoError(t, err)
		}
	}

	require.NoError(t, tw.Close())

	return &buf
}

type tarEntry struct {
	name     string
	mode     int64
	typeflag byte
	content  string
}

func TestExtractTar_SingleFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	archive := createTarArchive(t, []tarEntry{
		{name: "hello.txt", mode: 0644, typeflag: tar.TypeReg, content: "hello world"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello world", string(content))
}

func TestExtractTar_Directory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	archive := createTarArchive(t, []tarEntry{
		{name: "subdir/", mode: 0755, typeflag: tar.TypeDir},
		{name: "subdir/file.txt", mode: 0644, typeflag: tar.TypeReg, content: "in subdir"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "subdir"))
	require.NoError(t, err)
	require.True(t, info.IsDir())

	content, err := os.ReadFile(filepath.Join(dir, "subdir", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "in subdir", string(content))
}

func TestExtractTar_NestedStructure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	archive := createTarArchive(t, []tarEntry{
		{name: "a/", mode: 0755, typeflag: tar.TypeDir},
		{name: "a/b/", mode: 0755, typeflag: tar.TypeDir},
		{name: "a/b/c.txt", mode: 0644, typeflag: tar.TypeReg, content: "deep"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "a", "b", "c.txt"))
	require.NoError(t, err)
	require.Equal(t, "deep", string(content))
}

func TestExtractTar_EmptyArchive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	var buf bytes.Buffer

	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.Close())

	err := ExtractTar(&buf, dir)
	require.NoError(t, err)
}

func TestExtractTar_FileWithoutExplicitParentDir(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Create archive with a file in a subdirectory without an explicit dir entry
	archive := createTarArchive(t, []tarEntry{
		{name: "parent/child.txt", mode: 0644, typeflag: tar.TypeReg, content: "auto-created parent"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "parent", "child.txt"))
	require.NoError(t, err)
	require.Equal(t, "auto-created parent", string(content))
}

func TestExtractTar_MultipleFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	archive := createTarArchive(t, []tarEntry{
		{name: "a.txt", mode: 0644, typeflag: tar.TypeReg, content: "aaa"},
		{name: "b.txt", mode: 0644, typeflag: tar.TypeReg, content: "bbb"},
		{name: "c.txt", mode: 0644, typeflag: tar.TypeReg, content: "ccc"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	for _, tc := range []struct {
		name    string
		content string
	}{
		{"a.txt", "aaa"},
		{"b.txt", "bbb"},
		{"c.txt", "ccc"},
	} {
		content, err := os.ReadFile(filepath.Join(dir, tc.name))
		require.NoError(t, err)
		require.Equal(t, tc.content, string(content))
	}
}

func TestExtractTar_LargeFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	largeContent := strings.Repeat("x", 100000)
	archive := createTarArchive(t, []tarEntry{
		{name: "big.bin", mode: 0644, typeflag: tar.TypeReg, content: largeContent},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "big.bin"))
	require.NoError(t, err)
	require.Len(t, content, 100000)
}

func TestExtractTar_FilePermissions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	archive := createTarArchive(t, []tarEntry{
		{name: "exec.sh", mode: 0755, typeflag: tar.TypeReg, content: "#!/bin/sh"},
		{name: "readonly.txt", mode: 0444, typeflag: tar.TypeReg, content: "read only"},
	})

	err := ExtractTar(archive, dir)
	require.NoError(t, err)

	info, err := os.Stat(filepath.Join(dir, "exec.sh"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm())

	info, err = os.Stat(filepath.Join(dir, "readonly.txt"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0444), info.Mode().Perm())
}

func TestExtractTar_CorruptedArchive(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	err := ExtractTar(bytes.NewReader([]byte("not a tar")), dir)
	require.Error(t, err)
}

func TestExtractTar_DirCreationError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Make the target dir read-only so MkdirAll fails
	require.NoError(t, os.Chmod(dir, 0444))
	t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

	archive := createTarArchive(t, []tarEntry{
		{name: "sub/", mode: 0755, typeflag: tar.TypeDir},
	})

	err := ExtractTar(archive, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "creating directory")
}

func TestExtractTar_FileCreationError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Make the target dir read-only so file creation fails
	require.NoError(t, os.Chmod(dir, 0444))
	t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

	archive := createTarArchive(t, []tarEntry{
		{name: "file.txt", mode: 0644, typeflag: tar.TypeReg, content: "data"},
	})

	err := ExtractTar(archive, dir)
	require.Error(t, err)
}

func TestExtractTar_ParentDirCreationError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	// Make the target dir read-only so parent dir creation for nested file fails
	require.NoError(t, os.Chmod(dir, 0444))
	t.Cleanup(func() { _ = os.Chmod(dir, 0755) })

	archive := createTarArchive(t, []tarEntry{
		{name: "nested/file.txt", mode: 0644, typeflag: tar.TypeReg, content: "data"},
	})

	err := ExtractTar(archive, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "creating parent directory")
}
