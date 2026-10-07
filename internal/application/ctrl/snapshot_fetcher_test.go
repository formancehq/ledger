package ctrl

import (
	"bytes"
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

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// fileStreamScript drives a MockServerStreamingClient[snapshotpb.FetchFileResponse]:
// it returns `responses` in order, then io.EOF. If failAt >= 0, the first call
// at that index returns failErr; the failure is consumed so a retry can resume
// the script at the same response.
type fileStreamScript struct {
	responses []*snapshotpb.FetchFileResponse
	failAt    int
	failErr   error
}

func newFileStream(t *testing.T, script fileStreamScript) *MockServerStreamingClient[snapshotpb.FetchFileResponse] {
	t.Helper()

	s := NewMockServerStreamingClient[snapshotpb.FetchFileResponse](gomock.NewController(t))

	idx := 0
	failed := false
	s.EXPECT().Recv().DoAndReturn(func() (*snapshotpb.FetchFileResponse, error) {
		if script.failAt >= 0 && idx == script.failAt && !failed {
			failed = true

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

func fixedSnapshotClient(client snapshotpb.SnapshotServiceClient) snapshotClientSource {
	return func() (snapshotpb.SnapshotServiceClient, error) { return client, nil }
}

// recyclingSnapshotClients hands out `first` until it is retired, then
// `second`, the way the transport pool swaps a restarted peer connection.
type recyclingSnapshotClients struct {
	first, second snapshotpb.SnapshotServiceClient
	retired       atomic.Bool
	resolved      atomic.Int32
}

func (r *recyclingSnapshotClients) source() (snapshotpb.SnapshotServiceClient, error) {
	r.resolved.Add(1)

	if r.retired.Load() {
		return r.second, nil
	}

	return r.first, nil
}

// closingConnErr is what grpc-go returns on a call issued through a
// ClientConn that was closed under it.
var closingConnErr = status.Error(codes.Canceled, "grpc: the client connection is closing")

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
			Path: path,
			Size: uint64(len(data)),
		})

		streams[path] = newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: data, Eof: true, Sha256: fileSHA256(data)},
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
	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 2, retryCount: 5, fileRetryCount: 3}

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
	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 4, retryCount: 5, fileRetryCount: 3}

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
	corruptedData := []byte("corrupt-content")

	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: corruptedData, Eof: true, Sha256: fileSHA256(correctData)},
			},
			failAt: -1,
		}),
	}
	client, _ := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "data.bin", Size: uint64(len(correctData))},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "hash mismatch")
}

func TestGRPCSnapshotFetcher_RejectsNonLocalManifestPath(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "written-outside.txt")
	traversalPath, err := filepath.Rel(targetDir, outsidePath)
	require.NoError(t, err)

	client, clientState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: traversalPath, Size: 1},
			}},
		},
		nil,
		nil,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 1}
	_, err = fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.ErrorContains(t, err, "invalid snapshot path")
	require.Zero(t, clientState.fetchFileCalls.Load())
	require.Equal(t, int32(1), clientState.closeCalled.Load())
	require.NoFileExists(t, outsidePath)
}

// TestGRPCSnapshotFetcher_RejectsStagingPathCollision covers the parallel
// transfer hazard: "a.bin" is downloaded through the staging name "a.bin.tmp",
// so a manifest declaring both entries lets one transfer rename its bytes onto
// the file the other transfer is still writing. The whole manifest is rejected
// before any FetchFile call, which makes the outcome independent of how the
// two downloads interleave.
func TestGRPCSnapshotFetcher_RejectsStagingPathCollision(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()

	client, clientState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "a.bin", Size: 3},
				{Path: "a.bin" + stagingSuffix, Size: 3},
			}},
		},
		nil,
		nil,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 4, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.ErrorContains(t, err, "collides with the staging path")
	require.Zero(t, clientState.fetchFileCalls.Load())
	require.Equal(t, int32(1), clientState.closeCalled.Load())
	require.NoFileExists(t, filepath.Join(targetDir, "a.bin"))
	require.NoFileExists(t, filepath.Join(targetDir, "a.bin"+stagingSuffix))
}

// TestGRPCSnapshotFetcher_RejectsDuplicateManifestPath covers the other
// parallel hazard from the same validation pass: two entries naming one file
// would race on the same staging name and final rename.
func TestGRPCSnapshotFetcher_RejectsDuplicateManifestPath(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()

	client, clientState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "a.bin", Size: 3},
				{Path: "a.bin", Size: 4},
			}},
		},
		nil,
		nil,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 4, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.ErrorContains(t, err, "duplicate snapshot path")
	require.Zero(t, clientState.fetchFileCalls.Load())
	require.Equal(t, int32(1), clientState.closeCalled.Load())
	require.NoFileExists(t, filepath.Join(targetDir, "a.bin"))
}

func TestGRPCSnapshotFetcher_RejectsSymlinkEscape(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	outsideDir := t.TempDir()
	require.NoError(t, os.Symlink(outsideDir, filepath.Join(targetDir, "outside-link")))

	entryPath := filepath.Join("outside-link", "written-outside.txt")
	content := []byte("attacker-controlled")
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		entryPath: newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: content, Eof: true, Sha256: fileSHA256(content)},
			},
			failAt: -1,
		}),
	}
	client, clientState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: entryPath, Size: uint64(len(content))},
			}},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.ErrorContains(t, err, "creating parent directory")
	require.Equal(t, int32(1), clientState.fetchFileCalls.Load())
	require.NoFileExists(t, filepath.Join(outsideDir, "written-outside.txt"))
}

func TestGRPCSnapshotFetcher_MissingDigest(t *testing.T) {
	t.Parallel()

	data := []byte("content")
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: data, Eof: true},
			},
			failAt: -1,
		}),
	}
	client, _ := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "data.bin", Size: uint64(len(data))},
			}},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid or missing SHA-256 digest")
}

func TestGRPCSnapshotFetcher_SizeMismatch(t *testing.T) {
	t.Parallel()

	data := []byte("short")
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: data, Eof: true, Sha256: fileSHA256(data)},
			},
			failAt: -1,
		}),
	}
	client, _ := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "data.bin", Size: uint64(len(data) + 1)},
			}},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "size mismatch")
}

func TestGRPCSnapshotFetcher_RejectsExcessBytes(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	declared := []byte("data")
	excess := []byte("!")
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: declared},
				{Data: excess},
			},
			failAt: -1,
		}),
	}
	client, _ := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "data.bin", Size: uint64(len(declared))},
			}},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "got at least 5")
	require.NoFileExists(t, filepath.Join(targetDir, "data.bin.tmp"))
}

func TestGRPCSnapshotFetcher_RetryRewritesPartialFile(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	partial := []byte("stale")
	complete := []byte("complete-content")
	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"data.bin": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: partial},
				{Data: complete, Eof: true, Sha256: fileSHA256(complete)},
			},
			failAt:  1,
			failErr: status.Error(codes.Unavailable, "transient stream failure"),
		}),
	}
	client, csState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
				{Path: "data.bin", Size: uint64(len(complete))},
			}},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 2}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.NoError(t, err)
	require.Equal(t, int32(2), csState.fetchFileCalls.Load())

	data, err := os.ReadFile(filepath.Join(targetDir, "data.bin"))
	require.NoError(t, err)
	require.Equal(t, complete, data)
	require.NoFileExists(t, filepath.Join(targetDir, "data.bin.tmp"))
}

func TestGRPCSnapshotFetcher_UnavailableWrapsErrNotAvailable(t *testing.T) {
	t.Parallel()

	client, _ := newMockSnapshotClient(t,
		nil,
		status.Error(codes.Unavailable, "connection refused"),
		nil,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 5, fileRetryCount: 3}
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
	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 2, retryCount: 5, fileRetryCount: 3}

	_, err := fetcher.FetchSnapshot(t.Context(), dir, progress, 0)
	require.NoError(t, err)

	require.Equal(t, uint64(6), progress.BytesTotal())
	require.Equal(t, uint64(6), progress.BytesReceived())
	require.Equal(t, uint64(2), progress.FilesTotal())
	require.Equal(t, uint64(2), progress.FilesCompleted())
}

func TestGRPCSnapshotFetcher_RedownloadsCompletedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Pre-write a.txt as already completed from a previous attempt.
	aContent := []byte("aaa")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), aContent, 0644))

	bContent := []byte("bbb")

	streams := map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{
		"a.txt": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: aContent, Eof: true, Sha256: fileSHA256(aContent)},
			},
			failAt: -1,
		}),
		"b.txt": newFileStream(t, fileStreamScript{
			responses: []*snapshotpb.FetchFileResponse{
				{Data: bContent, Eof: true, Sha256: fileSHA256(bContent)},
			},
			failAt: -1,
		}),
	}
	client, csState := newMockSnapshotClient(t,
		&snapshotpb.PrepareSnapshotResponse{
			SessionId: "test-session",
			Manifest: &snapshotpb.SnapshotManifest{
				Files: []*snapshotpb.FileEntry{
					{Path: "a.txt", Size: uint64(len(aContent))},
					{Path: "b.txt", Size: uint64(len(bContent))},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 2, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)

	// Both files must be fetched because the manifest has no content digest
	// with which to trust a file created by an earlier session.
	require.Equal(t, int32(2), csState.fetchFileCalls.Load())

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
					{Path: "fail.txt", Size: 10},
				},
			},
		},
		nil,
		streams,
	)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 5, fileRetryCount: 3}
	_, err := fetcher.FetchSnapshot(t.Context(), t.TempDir(), nil, 0)
	require.Error(t, err)
	require.GreaterOrEqual(t, csState.closeCalled.Load(), int32(1))
}

// TestGRPCSnapshotFetcher_LogsThroughInjectedLogger pins the logging wiring:
// the fetcher must write through the logger it was constructed with, not
// through logging.FromContext on a context that carries no logger (which
// degrades to go-libs' bare logrus stderr fallback and produced mixed log
// formats in production). Reverting to FromContext keeps every other fetcher
// test green but fails this one, because the context passed here is bare.
func TestGRPCSnapshotFetcher_LogsThroughInjectedLogger(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	files := map[string][]byte{"a.txt": []byte("aaa")}

	var logs bytes.Buffer
	logger := logging.NewDefaultLogger(&logs, false, false, false)

	client, _ := buildMockClient(t, files)
	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logger, parallelism: 1, retryCount: 1, fileRetryCount: 1}

	_, err := fetcher.FetchSnapshot(t.Context(), dir, nil, 0)
	require.NoError(t, err)

	logged := logs.String()
	require.Contains(t, logged, "Requesting snapshot session from leader")
	require.Contains(t, logged, "Downloading snapshot file")
	require.Contains(t, logged, "Snapshot file downloaded")
}

// TestGRPCSnapshotFetcher_RetriesFileOnRecycledConnection covers the Raft
// transport restarting the shared peer connection while a file stream is in
// flight: the stream fails with Canceled, and the next attempt must run on
// the re-dialed connection and complete the file.
func TestGRPCSnapshotFetcher_RetriesFileOnRecycledConnection(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	content := []byte("complete-content")
	manifest := &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
		{Path: "data.bin", Size: uint64(len(content))},
	}}
	prepared := &snapshotpb.PrepareSnapshotResponse{SessionId: "test-session", Manifest: manifest}

	clients := &recyclingSnapshotClients{}

	// The stream delivers a partial chunk, then dies with the connection.
	closingStream := NewMockServerStreamingClient[snapshotpb.FetchFileResponse](gomock.NewController(t))
	gomock.InOrder(
		closingStream.EXPECT().Recv().Return(&snapshotpb.FetchFileResponse{Data: content[:4]}, nil),
		closingStream.EXPECT().Recv().DoAndReturn(func() (*snapshotpb.FetchFileResponse, error) {
			clients.retired.Store(true)

			return nil, closingConnErr
		}),
	)
	closingClient, closingState := newMockSnapshotClient(t, prepared, nil,
		map[string]*MockServerStreamingClient[snapshotpb.FetchFileResponse]{"data.bin": closingStream})

	freshClient, freshState := buildMockClient(t, map[string][]byte{"data.bin": content})

	clients.first, clients.second = closingClient, freshClient

	fetcher := &grpcSnapshotFetcher{clientSource: clients.source, logger: logging.Testing(), parallelism: 1, retryCount: 1, fileRetryCount: 2}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.NoError(t, err)

	require.Equal(t, int32(1), closingState.fetchFileCalls.Load())
	require.Equal(t, int32(1), freshState.fetchFileCalls.Load())
	require.Equal(t, int32(3), clients.resolved.Load(), "session + one resolve per file attempt")

	data, err := os.ReadFile(filepath.Join(targetDir, "data.bin"))
	require.NoError(t, err)
	require.Equal(t, content, data)
	require.NoFileExists(t, filepath.Join(targetDir, "data.bin.tmp"))
}

// TestGRPCSnapshotFetcher_RetriesSessionOnRecycledConnection covers the
// restart landing on PrepareSnapshot: the session attempt fails with
// Canceled and the next session must be opened on the re-dialed connection.
func TestGRPCSnapshotFetcher_RetriesSessionOnRecycledConnection(t *testing.T) {
	t.Parallel()

	targetDir := t.TempDir()
	content := []byte("complete-content")

	clients := &recyclingSnapshotClients{}

	closingClient := NewMockSnapshotServiceClient(gomock.NewController(t))
	closingClient.EXPECT().PrepareSnapshot(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *snapshotpb.PrepareSnapshotRequest, ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
			clients.retired.Store(true)

			return nil, closingConnErr
		}).Times(1)

	freshClient, freshState := buildMockClient(t, map[string][]byte{"data.bin": content})

	clients.first, clients.second = closingClient, freshClient

	fetcher := &grpcSnapshotFetcher{clientSource: clients.source, logger: logging.Testing(), parallelism: 1, retryCount: 2, fileRetryCount: 1}
	_, err := fetcher.FetchSnapshot(t.Context(), targetDir, nil, 0)
	require.NoError(t, err)

	require.Equal(t, int32(1), freshState.fetchFileCalls.Load())
	require.Equal(t, int32(1), freshState.closeCalled.Load())

	data, err := os.ReadFile(filepath.Join(targetDir, "data.bin"))
	require.NoError(t, err)
	require.Equal(t, content, data)
}

// A Canceled status caused by the caller's own context is not a transport
// hiccup: the fetch must stop instead of burning retries.
func TestGRPCSnapshotFetcher_CallerCancellationIsNotRetried(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	client := NewMockSnapshotServiceClient(gomock.NewController(t))
	client.EXPECT().PrepareSnapshot(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *snapshotpb.PrepareSnapshotRequest, ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
			cancel()

			return nil, status.FromContextError(context.Canceled).Err()
		}).Times(1)

	fetcher := &grpcSnapshotFetcher{clientSource: fixedSnapshotClient(client), logger: logging.Testing(), parallelism: 1, retryCount: 5, fileRetryCount: 1}
	// Times(1) on PrepareSnapshot is the no-retry assertion. The error
	// surfaces either as the gRPC status or as the heartbeat wrapper's
	// context error, depending on which select arm wins.
	_, err := fetcher.FetchSnapshot(ctx, t.TempDir(), nil, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled, err)
}
