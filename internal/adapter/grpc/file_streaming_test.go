package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

func TestBuildManifest_SingleFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello world"), 0644))

	manifest, err := buildManifest(context.Background(), dir)
	require.NoError(t, err)
	require.Len(t, manifest.GetFiles(), 1)

	entry := manifest.GetFiles()[0]
	require.Equal(t, "hello.txt", entry.GetPath())
	require.Equal(t, uint64(11), entry.GetSize())
}

func TestBuildManifest_NestedDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "root.txt"), []byte("root"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "nested.txt"), []byte("nested"), 0644))

	manifest, err := buildManifest(context.Background(), dir)
	require.NoError(t, err)
	require.Len(t, manifest.GetFiles(), 2)

	paths := make(map[string]uint64, len(manifest.GetFiles()))
	for _, e := range manifest.GetFiles() {
		paths[e.GetPath()] = e.GetSize()
	}

	require.Equal(t, uint64(4), paths["root.txt"])
	require.Equal(t, uint64(6), paths[filepath.Join("sub", "nested.txt")])
}

func TestBuildManifest_EmptyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	manifest, err := buildManifest(context.Background(), dir)
	require.NoError(t, err)
	require.Empty(t, manifest.GetFiles())
}

func TestBuildManifest_NonExistentDirectory(t *testing.T) {
	t.Parallel()

	_, err := buildManifest(context.Background(), "/nonexistent/path/does/not/exist")
	require.Error(t, err)
}

func TestBuildManifest_CanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := buildManifest(ctx, t.TempDir())
	require.ErrorIs(t, err, context.Canceled)
}

func TestStreamOneFile_Content(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), []byte("hello world"), 0644))

	var chunks []*snapshotpb.FetchFileResponse
	buf := make([]byte, defaultChunkSize)

	err := streamOneFile(dir, "data.bin", buf, func(resp *snapshotpb.FetchFileResponse) error {
		chunks = append(chunks, &snapshotpb.FetchFileResponse{
			Data:   append([]byte(nil), resp.GetData()...),
			Eof:    resp.GetEof(),
			Sha256: resp.GetSha256(),
		})

		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	// Last chunk should have EOF.
	require.True(t, chunks[len(chunks)-1].GetEof())

	// Reassemble and verify.
	var data []byte
	for _, c := range chunks {
		data = append(data, c.GetData()...)
	}

	require.Equal(t, "hello world", string(data))
	digest := sha256.Sum256(data)
	require.Equal(t, hex.EncodeToString(digest[:]), chunks[len(chunks)-1].GetSha256())
}

func TestStreamOneFile_Empty(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "empty.txt"), nil, 0644))

	var chunks []*snapshotpb.FetchFileResponse
	buf := make([]byte, defaultChunkSize)

	err := streamOneFile(dir, "empty.txt", buf, func(resp *snapshotpb.FetchFileResponse) error {
		chunks = append(chunks, resp)

		return nil
	})
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.True(t, chunks[0].GetEof())
	digest := sha256.Sum256(nil)
	require.Equal(t, hex.EncodeToString(digest[:]), chunks[0].GetSha256())
}
