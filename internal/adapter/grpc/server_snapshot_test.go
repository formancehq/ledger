package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

const bufSize = 1024 * 1024

// testSnapshotServer is a minimal SnapshotService that serves files from a
// static directory (no dal.Store dependency). Used for integration testing.
type testSnapshotServer struct {
	snapshotpb.UnimplementedSnapshotServiceServer

	checkpointDir string
	sessions      *snapshotSessionStore
}

func newTestSnapshotServer(checkpointDir string) *testSnapshotServer {
	return &testSnapshotServer{
		checkpointDir: checkpointDir,
		sessions:      newSnapshotSessionStore(nil, noopLogger{}, defaultSessionTTL),
	}
}

// noopLogger stays hand-rolled: logging.Logger lives in
// github.com/formancehq/go-libs and has no mockgen directive upstream. A
// silent fixture is simpler than a vendor mock here.
type noopLogger struct{}

func (noopLogger) Enabled(_ logging.Level) bool                 { return false }
func (noopLogger) Tracef(_ string, _ ...any)                    {}
func (noopLogger) Debugf(_ string, _ ...any)                    {}
func (noopLogger) Infof(_ string, _ ...any)                     {}
func (noopLogger) Errorf(_ string, _ ...any)                    {}
func (noopLogger) Trace(_ ...any)                               {}
func (noopLogger) Debug(_ ...any)                               {}
func (noopLogger) Info(_ ...any)                                {}
func (noopLogger) Error(_ ...any)                               {}
func (noopLogger) WithField(_ string, _ any) logging.Logger     { return noopLogger{} }
func (noopLogger) WithFields(_ map[string]any) logging.Logger   { return noopLogger{} }
func (noopLogger) WithContext(_ context.Context) logging.Logger { return noopLogger{} }
func (noopLogger) Writer() io.Writer                            { return io.Discard }

func (s *testSnapshotServer) PrepareSnapshot(_ context.Context, _ *snapshotpb.PrepareSnapshotRequest) (*snapshotpb.PrepareSnapshotResponse, error) {
	manifest, err := buildManifest(s.checkpointDir)
	if err != nil {
		return nil, err
	}

	sessionID, err := s.sessions.create("test-sync", s.checkpointDir)
	if err != nil {
		return nil, err
	}

	return &snapshotpb.PrepareSnapshotResponse{
		SessionId: sessionID,
		Manifest:  manifest,
	}, nil
}

func (s *testSnapshotServer) FetchFile(req *snapshotpb.FetchFileRequest, stream ggrpc.ServerStreamingServer[snapshotpb.FetchFileResponse]) error {
	session, ok := s.sessions.get(req.GetSessionId())
	if !ok {
		return status.Errorf(codes.NotFound, "session not found")
	}

	buf := make([]byte, defaultChunkSize)

	return streamOneFile(session.checkpointPath, req.GetPath(), buf, func(resp *snapshotpb.FetchFileResponse) error {
		return stream.Send(resp)
	})
}

func (s *testSnapshotServer) CloseSession(_ context.Context, req *snapshotpb.CloseSessionRequest) (*snapshotpb.CloseSessionResponse, error) {
	s.sessions.remove(req.GetSessionId())

	return &snapshotpb.CloseSessionResponse{}, nil
}

// setupBufconn creates an in-process gRPC server+client pair using bufconn.
func setupBufconn(t *testing.T, checkpointDir string) snapshotpb.SnapshotServiceClient {
	t.Helper()

	lis := bufconn.Listen(bufSize)

	server := ggrpc.NewServer()
	testServer := newTestSnapshotServer(checkpointDir)
	snapshotpb.RegisterSnapshotServiceServer(server, testServer)

	go func() {
		_ = server.Serve(lis) // error expected on GracefulStop
	}()

	t.Cleanup(func() {
		server.GracefulStop()
		testServer.sessions.stop()
	})

	conn, err := ggrpc.NewClient(
		"passthrough:///bufconn",
		ggrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	return snapshotpb.NewSnapshotServiceClient(conn)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)

	return hex.EncodeToString(h[:])
}

func TestSnapshotService_FullRoundTrip(t *testing.T) {
	t.Parallel()

	checkpointDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "MANIFEST-000001"), []byte("manifest-data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "000001.sst"), []byte("sst-block-data"), 0644))

	client := setupBufconn(t, checkpointDir)

	// PrepareSnapshot.
	resp, err := client.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetSessionId())
	require.Len(t, resp.GetManifest().GetFiles(), 2)

	// FetchFile for each file.
	targetDir := t.TempDir()

	for _, entry := range resp.GetManifest().GetFiles() {
		stream, err := client.FetchFile(t.Context(), &snapshotpb.FetchFileRequest{
			SessionId: resp.GetSessionId(),
			Path:      entry.GetPath(),
		})
		require.NoError(t, err)

		var fileData []byte

		for {
			chunk, err := stream.Recv()
			if err != nil {
				break
			}

			fileData = append(fileData, chunk.GetData()...)

			if chunk.GetEof() {
				break
			}
		}

		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(targetDir, entry.GetPath())), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(targetDir, entry.GetPath()), fileData, 0644))
		require.Equal(t, entry.GetSha256(), sha256Hex(fileData))
	}

	// CloseSession.
	_, err = client.CloseSession(t.Context(), &snapshotpb.CloseSessionRequest{SessionId: resp.GetSessionId()})
	require.NoError(t, err)
}

func TestSnapshotService_ParallelFetchFile(t *testing.T) {
	t.Parallel()

	checkpointDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "b.txt"), []byte("bbb"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "c.txt"), []byte("ccc"), 0644))

	client := setupBufconn(t, checkpointDir)

	resp, err := client.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{})
	require.NoError(t, err)

	// Fetch all files in parallel.
	type result struct {
		path string
		data []byte
	}

	results := make(chan result, len(resp.GetManifest().GetFiles()))

	for _, entry := range resp.GetManifest().GetFiles() {
		go func() {
			stream, err := client.FetchFile(t.Context(), &snapshotpb.FetchFileRequest{
				SessionId: resp.GetSessionId(),
				Path:      entry.GetPath(),
			})
			require.NoError(t, err)

			var data []byte

			for {
				chunk, err := stream.Recv()
				if errors.Is(err, io.EOF) || (err == nil && chunk.GetEof()) {
					if err == nil {
						data = append(data, chunk.GetData()...)
					}

					break
				}

				require.NoError(t, err)
				data = append(data, chunk.GetData()...)
			}

			results <- result{path: entry.GetPath(), data: data}
		}()
	}

	received := make(map[string][]byte)
	for range resp.GetManifest().GetFiles() {
		r := <-results
		received[r.path] = r.data
	}

	require.Len(t, received, 3)
	require.Equal(t, []byte("aaa"), received["a.txt"])
	require.Equal(t, []byte("bbb"), received["b.txt"])
	require.Equal(t, []byte("ccc"), received["c.txt"])

	_, err = client.CloseSession(t.Context(), &snapshotpb.CloseSessionRequest{SessionId: resp.GetSessionId()})
	require.NoError(t, err)
}

func TestSnapshotService_InvalidSessionID(t *testing.T) {
	t.Parallel()

	client := setupBufconn(t, t.TempDir())

	// FetchFile with server-streaming: the error surfaces on Recv, not on the initial call.
	stream, err := client.FetchFile(t.Context(), &snapshotpb.FetchFileRequest{
		SessionId: "nonexistent",
		Path:      "foo.txt",
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, s.Code())
}

func TestSnapshotService_EmptyCheckpoint(t *testing.T) {
	t.Parallel()

	client := setupBufconn(t, t.TempDir())

	resp, err := client.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetSessionId())
	require.Empty(t, resp.GetManifest().GetFiles())

	_, err = client.CloseSession(t.Context(), &snapshotpb.CloseSessionRequest{SessionId: resp.GetSessionId()})
	require.NoError(t, err)
}
