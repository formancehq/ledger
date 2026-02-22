package application

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStreamDirAsTar_SingleFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0644))

	var chunks []TarStreamChunk
	err := StreamDirAsTar(dir, 0, func(c TarStreamChunk) error {
		chunks = append(chunks, TarStreamChunk{
			Data:          append([]byte(nil), c.Data...),
			ChunkOffset:   c.ChunkOffset,
			IsFirst:       c.IsFirst,
			IsEOF:         c.IsEOF,
			ContentSHA256: c.ContentSHA256,
			ContentSize:   c.ContentSize,
		})
		return nil
	})
	require.NoError(t, err)

	// Should have at least 2 chunks: data + EOF
	require.GreaterOrEqual(t, len(chunks), 2)

	// First chunk should be marked as first
	require.True(t, chunks[0].IsFirst)
	require.False(t, chunks[0].IsEOF)

	// Last chunk should be EOF
	last := chunks[len(chunks)-1]
	require.True(t, last.IsEOF)
	require.NotEmpty(t, last.ContentSHA256)
	require.Greater(t, last.ContentSize, uint64(0))

	// Reassemble and verify tar content
	var tarData bytes.Buffer
	for _, c := range chunks {
		tarData.Write(c.Data)
	}

	tr := tar.NewReader(&tarData)
	foundFile := false
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if header.Name == "hello.txt" {
			foundFile = true
			data, err := io.ReadAll(tr)
			require.NoError(t, err)
			require.Equal(t, "hello world", string(data))
		}
	}
	require.True(t, foundFile, "expected hello.txt in tar archive")
}

func TestStreamDirAsTar_NestedDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("nested"), 0644))

	var chunks []TarStreamChunk
	err := StreamDirAsTar(dir, 0, func(c TarStreamChunk) error {
		chunks = append(chunks, TarStreamChunk{
			Data:          append([]byte(nil), c.Data...),
			ChunkOffset:   c.ChunkOffset,
			IsFirst:       c.IsFirst,
			IsEOF:         c.IsEOF,
			ContentSHA256: c.ContentSHA256,
			ContentSize:   c.ContentSize,
		})
		return nil
	})
	require.NoError(t, err)

	var tarData bytes.Buffer
	for _, c := range chunks {
		tarData.Write(c.Data)
	}

	tr := tar.NewReader(&tarData)
	files := make(map[string]string)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		if !header.FileInfo().IsDir() {
			data, err := io.ReadAll(tr)
			require.NoError(t, err)
			files[header.Name] = string(data)
		}
	}

	require.Equal(t, "root", files["root.txt"])
	require.Equal(t, "nested", files[filepath.Join("sub", "nested.txt")])
}

func TestStreamDirAsTar_EmptyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	var chunks []TarStreamChunk
	err := StreamDirAsTar(dir, 0, func(c TarStreamChunk) error {
		chunks = append(chunks, TarStreamChunk{
			Data:          append([]byte(nil), c.Data...),
			ChunkOffset:   c.ChunkOffset,
			IsFirst:       c.IsFirst,
			IsEOF:         c.IsEOF,
			ContentSHA256: c.ContentSHA256,
			ContentSize:   c.ContentSize,
		})
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	// Last chunk should be EOF
	last := chunks[len(chunks)-1]
	require.True(t, last.IsEOF)
	require.NotEmpty(t, last.ContentSHA256)
}

func TestStreamDirAsTar_WithOffset(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), bytes.Repeat([]byte("x"), 100000), 0644))

	// First, stream fully to get total size
	var fullChunks []TarStreamChunk
	err := StreamDirAsTar(dir, 0, func(c TarStreamChunk) error {
		fullChunks = append(fullChunks, TarStreamChunk{
			Data:          append([]byte(nil), c.Data...),
			ChunkOffset:   c.ChunkOffset,
			IsFirst:       c.IsFirst,
			IsEOF:         c.IsEOF,
			ContentSHA256: c.ContentSHA256,
			ContentSize:   c.ContentSize,
		})
		return nil
	})
	require.NoError(t, err)

	// Now stream with an offset
	var offsetChunks []TarStreamChunk
	err = StreamDirAsTar(dir, 1024, func(c TarStreamChunk) error {
		offsetChunks = append(offsetChunks, TarStreamChunk{
			Data:          append([]byte(nil), c.Data...),
			ChunkOffset:   c.ChunkOffset,
			IsFirst:       c.IsFirst,
			IsEOF:         c.IsEOF,
			ContentSHA256: c.ContentSHA256,
			ContentSize:   c.ContentSize,
		})
		return nil
	})
	require.NoError(t, err)

	// Offset chunks should have less total data
	var fullSize, offsetSize uint64
	for _, c := range fullChunks {
		fullSize += uint64(len(c.Data))
	}
	for _, c := range offsetChunks {
		offsetSize += uint64(len(c.Data))
	}
	require.Less(t, offsetSize, fullSize)
}

func TestStreamDirAsTar_NonExistentDirectory(t *testing.T) {
	t.Parallel()

	err := StreamDirAsTar("/nonexistent/path/does/not/exist", 0, func(c TarStreamChunk) error {
		return nil
	})
	require.Error(t, err)
}
