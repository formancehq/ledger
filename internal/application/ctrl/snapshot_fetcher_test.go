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
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// fileStreamScript drives a MockServerStreamingClient[snapshotpb.FetchFileResponse]:
// it returns `responses` in order, then io.EOF. If failAt >= 0, the call at
// that index returns failErr instead of the scripted response.
type fileStreamScript struct {
	responses []*snapshotpb.FetchFileResponse
	failAt    int
	failErr   error
}

func newFileStream(t *testing.T, script fileStreamScript) *MockServerStreamingClient[snapshotpb.FetchFileResponse] {
	t.Helper()

	s := NewMockServerStreamingClient[snapshotpb.FetchFileResponse](gomock.NewController(t))

	idx := 0
	s.EXPECT().Recv().DoAndReturn(func() (*snapshotpb.FetchFileResponse, error) {
		if script.failAt >= 0 && idx == script.failAt {
			return nil, script.failErr
		}

		if idx >= len(script.responses) {
			return nil, io.EOF
		}

		resp := script.responses[idx]
		idx++

		return resp, nil
	}).AnyTimes()

	return s
}

// snapshotClientState backs a MockSnapshotServiceClient so tests can keep the
// per-path FetchFile semantics, plus the call-count counters the originals
// exposed as fields.
type snapshotClientState struct {
	fileStreams    map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]
	closeErr       error
	closeCalled    atomic.Int32
	fetchFileCalls atomic.Int32
}

func newMockSnapshotClient(t *testing.T, prepareResp *snapshotpb.PrepareSnapshotResponse, prepareErr error, streams map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]) (*MockSnapshotServiceClient, *snapshotClientState) {
	t.Helper()

	s := &snapshotClientState{fileStreams: streams}
	c := NewMockSnapshotServiceClient(gomock.NewController(t))

	c.EXPECT().PrepareSnapshot(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *snapshotpb.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
			if prepareErr != nil {
				return nil, prepareErr
			}

			return prepareResp, nil
		}).AnyTimes()

	c.EXPECT().FetchFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *snapshotpb.FetchFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[snapshotpb.FetchFileResponse], error) {
			s.fetchFileCalls.Add(1)

			stream, ok := s.fileStreams[in.GetPath()]
			if !ok {
				return nil, errors.New("no stream for path: " + in.GetPath())
			}

			return stream, nil
		}).AnyTimes()

	c.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *snapshotpb.CloseSessionRequest, _ ...grpc.CallOption) (*snapshotpb.CloseSessionResponse, error) {
			s.closeCalled.Add(1)

			return &snapshotpb.CloseSessionResponse{}, s.closeErr
		}).AnyTimes()

	return c, s
}

func fileSHA256(data []byte) string {
	h := sha256.Sum256(data)

	return hex.EncodeToString(h[:])
}

func buildMockClient(t *testing.T, files map[string][]byte) (*MockSnapshotServiceClient, *snapshotClientState) {
	t.Helper()

	var entries []*snapshotpb.FileEntry

	streams := make(map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse], len(files))

	for path, data := range files {
		entries = append(entries, &snapshotpb.FileEntry{
			Path:   path,
			Size:   uint64(len(data)),
			Sha256: fileSHA256(data),
		})

		streams[path] = newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: data, Eof: true},
			},
			failAt: -1,
		})
	}

	return newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest:  &snapshotpb.SnapshotManifest{Files: entries},
		},
		nil,
		streams,
	)
}

func TestGRPCSnapshotFetcher_HappyPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string][]byte{
		"a.txt": []byte("aaa"),
		"b.txt": []byte("bbb"),
	}

	client, csState := buildMockClient(t, files)
	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 2, retryCount: 5, fileRetryCount: 3}

	size, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(6), size)

	for path, expected := range files {
		data, err := os.ReadFile(filepath.Join(dir, path))
		require.NoError(t, err)
		require.Equal(t, expected, data)
	}

	require.Equal(t, int32(1), csState.closeCalled.Load())
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

	client, csState := buildMockClient(t, files)
	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 4, retryCount: 5, fileRetryCount: 3}

	size, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(12), size)

	// All 4 files fetched.
	require.Equal(t, int32(4), csState.fetchFileCalls.Load())
}

func TestGRPCSnapshotFetcher_HashMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	correctData := []byte("correct-content")

	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: []byte("corrupted-data!!"), Eof: true},
			},
			failAt: -1,
		}),
	}
	client, _ := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "data.bin", Size: uint64(len(correctData)), Sha256: fileSHA256(correctData)},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash mismatch")
}

func TestGRPCSnapshotFetcher_UnavailableWrapsErrNotAvailable(t *testing.T) {
	t.Parallel()

	client, _ := newMockSnapshotClient(t,
		nil,
		status.Error(codes.Unavailable, "connection refused"),
		nil,
	)

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

	client, _ := buildMockClient(t, files)
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

	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"b.txt": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: bContent, Eof: true},
			},
			failAt: -1,
		}),
	}
	client, csState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "a.txt", Size: uint64(len(aContent)), Sha256: fileSHA256(aContent)},
					{Path: "b.txt", Size: uint64(len(bContent)), Sha256: fileSHA256(bContent)},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 2, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)

	// Only b.txt should have been fetched (a.txt was already on disk).
	require.Equal(t, int32(1), csState.fetchFileCalls.Load())

	data, err := os.ReadFile(filepath.Join(dir, "b.txt"))
	require.NoError(t, err)
	require.Equal(t, bContent, data)
}

func TestGRPCSnapshotFetcher_CloseSessionAlwaysCalled(t *testing.T) {
	t.Parallel()

	// Even when FetchFile fails, CloseSession should be called.
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"fail.txt": newFileStream(t, fileStreamScript{
			failAt:  0,
			failErr: status.Error(codes.PermissionDenied, "denied"),
		}),
	}
	client, csState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "fail.txt", Size: 10, Sha256: "deadbeef"},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{client: client, parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.GreaterOrEqual(t, csState.closeCalled.Load(), int32(1))
}
