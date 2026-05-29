package ctrl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// mockFileStream implements grpc.ServerStreamingClient[snapshotpb.FetchFileResponse].
type mockFileStream struct {
	responses []*snapshotpb.FetchFileResponse
	index     int
	failAt    int
	failErr   error
	ctx       context.Context
}

func (s *mockFileStream) Recv() (*snapshotpb.FetchFileResponse, error) {
	if s.failAt >= 0 && s.index == s.failAt {
		return nil, s.failErr
	}

	if s.index >= len(s.responses) {
		return nil, io.EOF
	}

	resp := s.responses[s.index]
	s.index++

	return resp, nil
}

func (s *mockFileStream) Header() (metadata.MD, error) { return nil, nil }
func (s *mockFileStream) Trailer() metadata.MD         { return nil }
func (s *mockFileStream) CloseSend() error             { return nil }
func (s *mockFileStream) Context() context.Context     { return s.ctx }
func (s *mockFileStream) SendMsg(any) error            { return nil }
func (s *mockFileStream) RecvMsg(any) error            { return nil }

// mockSnapshotClient implements snapshotpb.SnapshotServiceClient.
type mockSnapshotClient struct {
	prepareResp    *snapshotpb.PrepareSnapshotResponse
	prepareErr     error
	fileStreams    map[string]*mockFileStream // path → stream
	closeErr       error
	closeCalled    atomic.Int32
	fetchFileCalls atomic.Int32
}

func (c *mockSnapshotClient) PrepareSnapshot(_ context.Context, _ *snapshotpb.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}

	return c.prepareResp, nil
}

func (c *mockSnapshotClient) FetchFile(ctx context.Context, in *snapshotpb.FetchFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[snapshotpb.FetchFileResponse], error) {
	c.fetchFileCalls.Add(1)

	stream, ok := c.fileStreams[in.GetPath()]
	if !ok {
		return nil, errors.New("no stream for path: " + in.GetPath())
	}

	stream.ctx = ctx

	return stream, nil
}

func (c *mockSnapshotClient) CloseSession(_ context.Context, _ *snapshotpb.CloseSessionRequest, _ ...grpc.CallOption) (*snapshotpb.CloseSessionResponse, error) {
	c.closeCalled.Add(1)

	return &snapshotpb.CloseSessionResponse{}, c.closeErr
}

func fileSHA256(data []byte) string {
	h := sha256.Sum256(data)

	return hex.EncodeToString(h[:])
}

func buildMockClient(files map[string][]byte) *mockSnapshotClient {
	var entries []*snapshotpb.FileEntry

	streams := make(map[string]*mockFileStream, len(files))

	for path, data := range files {
		entries = append(entries, &snapshotpb.FileEntry{
			Path:   path,
			Size:   uint64(len(data)),
			Sha256: fileSHA256(data),
		})

		streams[path] = &mockFileStream{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: data, Eof: true},
			},
			failAt: -1,
		}
	}

	return &mockSnapshotClient{
		prepareResp: &snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest:  &snapshotpb.SnapshotManifest{Files: entries},
		},
		fileStreams: streams,
	}
}

func TestGRPCSnapshotFetcher_HappyPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
	}

	client := buildMockClient(files)
	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 2, retryCount: 5, fileRetryCount: 3}

	size, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(6), size)

	for path, expected := range files {
		data, err := os.ReadFile(filepath.Join(dir, path))
		require.NoError(t, err)
		require.Equal(t, expected, data)
	}

	require.Equal(t, int32(1), client.closeCalled.Load())
}

func TestGRPCSnapshotFetcher_ParallelFetch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
		"c.txt": []byte("ccc"),
		"d.txt": []byte("ddd"),
	}

	client := buildMockClient(files)
	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 4, retryCount: 5, fileRetryCount: 3}

	size, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(12), size)

	// All 4 files fetched.
	require.Equal(t, int32(4), client.fetchFileCalls.Load())
}

func TestGRPCSnapshotFetcher_HashMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	correctData := []byte("correct-content")

	client := &mockSnapshotClient{
		prepareResp: &snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "data.bin", Size: uint64(len(correctData)), Sha256: fileSHA256(correctData)},
				},
			},
		},
		fileStreams: map[string]*mockFileStream{
			"data.bin": {
				responses: []*snapshotpb.FetchFileResponse{
					{Data: []byte("corrupted-data!!"), Eof: true},
				},
				failAt: -1,
			},
		},
	}

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash mismatch")
}

func TestGRPCSnapshotFetcher_UnavailableWrapsErrNotAvailable(t *testing.T) {
	t.Parallel()

	client := &mockSnapshotClient{
		prepareErr: status.Error(codes.Unavailable, "connection refused"),
	}

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, state.ErrNotAvailable)
}

func TestGRPCSnapshotFetcher_ProgressTracking(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
	}

	client := buildMockClient(files)
	progress := state.NewSyncProgress()
	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 2, retryCount: 5, fileRetryCount: 3}

	_, err := fetcher.FetchSnapshot(t.Context(), dir, progress, 0)
	require.NoError(t, err)

	require.Equal(t, uint64(6), progress.BytesTotal())
	require.Equal(t, uint64(6), progress.BytesReceived())
	require.Equal(t, uint64(2), progress.FilesTotal())
	require.Equal(t, uint64(2), progress.FilesCompleted())
}

func TestGRPCSnapshotFetcher_ResumeSkipsCompletedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Pre-write a.txt as already completed from a previous attempt.
	aContent := []byte("aaa")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), aContent, 0644))

	bContent := []byte("bbb")

	client := &mockSnapshotClient{
		prepareResp: &snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "a.txt", Size: uint64(len(aContent)), Sha256: fileSHA256(aContent)},
					{Path: "b.txt", Size: uint64(len(bContent)), Sha256: fileSHA256(bContent)},
				},
			},
		},
		fileStreams: map[string]*mockFileStream{
			"b.txt": {
				responses: []*snapshotpb.FetchFileResponse{
					{Data: bContent, Eof: true},
				},
				failAt: -1,
			},
		},
	}

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 2, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)

	// Only b.txt should have been fetched (a.txt was already on disk).
	require.Equal(t, int32(1), client.fetchFileCalls.Load())

	data, err := os.ReadFile(filepath.Join(dir, "b.txt"))
	require.NoError(t, err)
	require.Equal(t, bContent, data)
}

func TestGRPCSnapshotFetcher_CloseSessionAlwaysCalled(t *testing.T) {
	t.Parallel()

	// Even when FetchFile fails, CloseSession should be called.
	client := &mockSnapshotClient{
		prepareResp: &snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "fail.txt", Size: 10, Sha256: "deadbeef"},
				},
			},
		},
		fileStreams: map[string]*mockFileStream{
			"fail.txt": {
				failAt:  0,
				failErr: status.Error(codes.PermissionDenied, "denied"),
			},
		},
	}

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.GreaterOrEqual(t, client.closeCalled.Load(), int32(1))
}
