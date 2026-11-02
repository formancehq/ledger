package ctrl

import (
	"context"
	"os"
	"path/filepath"
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

// Exercise donor loss through the real session, file, and staging machinery.
// Only the peer RPC boundary is mocked; a partial file must actually reach disk
// before its donor disappears, and the surviving donor must replace the session.
func TestFallbackSnapshotFetcher_DonorLostDuringCheckpoint(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"prepare", "partial file"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			failed := NewMockSnapshotServiceClient(controller)
			survivor := NewMockSnapshotServiceClient(controller)
			dir := t.TempDir()
			progress := state.NewSyncProgress()
			const minIndex = uint64(42)
			var lost bool

			for _, client := range []*MockSnapshotServiceClient{failed, survivor} {
				client.EXPECT().CheckSnapshotReadiness(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *snapshotpb.CheckSnapshotReadinessRequest, _ ...grpc.CallOption) (*snapshotpb.CheckSnapshotReadinessResponse, error) {
						require.Equal(t, minIndex, req.GetMinAppliedIndex())

						return &snapshotpb.CheckSnapshotReadinessResponse{Ready: true}, nil
					}).Times(1)
			}

			failed.EXPECT().PrepareSnapshot(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *snapshotpb.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
					require.Equal(t, minIndex, req.GetMinAppliedIndex())
					if phase == "prepare" {
						lost = true

						return nil, status.Error(codes.Unavailable, "donor stopped during preparation")
					}

					return &snapshotpb.PrepareSnapshotResponse{
						SessionId: "lost-donor",
						Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
							{Path: "orphan.bin", Size: 3},
							{Path: "data.bin", Size: 8},
						}},
					}, nil
				}).Times(1)

			if phase == "partial file" {
				complete := newFileStream(t, fileStreamScript{
					responses: []*snapshotpb.FetchFileResponse{{Data: []byte("old"), Eof: true, Sha256: fileSHA256([]byte("old"))}},
					failAt:    -1,
				})
				partial := NewMockServerStreamingClient[snapshotpb.FetchFileResponse](controller)
				gomock.InOrder(
					partial.EXPECT().Recv().Return(&snapshotpb.FetchFileResponse{Data: []byte("partial")}, nil),
					partial.EXPECT().Recv().DoAndReturn(func() (*snapshotpb.FetchFileResponse, error) {
						data, err := os.ReadFile(filepath.Join(dir, "data.bin.tmp"))
						require.NoError(t, err)
						require.Equal(t, []byte("partial"), data)
						require.Equal(t, uint64(10), progress.BytesReceived())
						require.Equal(t, uint64(1), progress.FilesCompleted())
						lost = true

						return nil, status.Error(codes.Unavailable, "donor stopped after partial transfer")
					}),
				)
				failed.EXPECT().FetchFile(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *snapshotpb.FetchFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[snapshotpb.FetchFileResponse], error) {
						require.Equal(t, "lost-donor", req.GetSessionId())
						if req.GetPath() == "orphan.bin" {
							return complete, nil
						}
						require.Equal(t, "data.bin", req.GetPath())

						return partial, nil
					}).Times(2)
				failed.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *snapshotpb.CloseSessionRequest, _ ...grpc.CallOption) (*snapshotpb.CloseSessionResponse, error) {
						require.Equal(t, "lost-donor", req.GetSessionId())
						require.NoFileExists(t, filepath.Join(dir, "data.bin.tmp"))

						return nil, status.Error(codes.Unavailable, "donor remains down")
					}).Times(1)
			}

			survivor.EXPECT().PrepareSnapshot(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *snapshotpb.PrepareSnapshotRequest, _ ...grpc.CallOption) (*snapshotpb.PrepareSnapshotResponse, error) {
					require.Equal(t, minIndex, req.GetMinAppliedIndex())

					return &snapshotpb.PrepareSnapshotResponse{
						SessionId: "surviving-donor",
						Manifest: &snapshotpb.SnapshotManifest{Files: []*snapshotpb.FileEntry{
							{Path: "data.bin", Size: 7},
						}},
					}, nil
				}).Times(1)
			stream := newFileStream(t, fileStreamScript{
				responses: []*snapshotpb.FetchFileResponse{{Data: []byte("current"), Eof: true, Sha256: fileSHA256([]byte("current"))}},
				failAt:    -1,
			})
			survivor.EXPECT().FetchFile(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *snapshotpb.FetchFileRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[snapshotpb.FetchFileResponse], error) {
					require.Equal(t, "surviving-donor", req.GetSessionId())
					require.Equal(t, "data.bin", req.GetPath())

					return stream, nil
				}).Times(1)
			survivor.EXPECT().CloseSession(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *snapshotpb.CloseSessionRequest, _ ...grpc.CallOption) (*snapshotpb.CloseSessionResponse, error) {
					require.Equal(t, "surviving-donor", req.GetSessionId())

					return &snapshotpb.CloseSessionResponse{}, nil
				}).Times(1)

			fetcher := &fallbackSnapshotFetcher{logger: logging.Testing()}
			for i, client := range []*MockSnapshotServiceClient{failed, survivor} {
				fetcher.candidates = append(fetcher.candidates, snapshotDonorCandidate{
					id: uint64(i + 1),
					newFetch: func() (state.SnapshotFetcher, error) {
						if i == 1 {
							require.True(t, lost, "the original donor must fail before fallback")
							entries, err := os.ReadDir(dir)
							require.NoError(t, err)
							require.Empty(t, entries, "the failed session must be discarded before preparing another")
							require.Zero(t, progress.BytesReceived())
							require.Zero(t, progress.BytesTotal())
							require.Zero(t, progress.FilesCompleted())
							require.Zero(t, progress.FilesTotal())
						}

						return &grpcSnapshotFetcher{
							clientSource: fixedSnapshotClient(client), logger: logging.Testing(),
							parallelism: 1, retryCount: 1, fileRetryCount: 1, checkReadiness: true,
						}, nil
					},
				})
			}

			size, err := fetcher.FetchSnapshot(t.Context(), dir, progress, minIndex)
			require.NoError(t, err)
			require.Equal(t, uint64(7), size)
			data, err := os.ReadFile(filepath.Join(dir, "data.bin"))
			require.NoError(t, err)
			require.Equal(t, []byte("current"), data)
			require.NoFileExists(t, filepath.Join(dir, "data.bin.tmp"))
			require.NoFileExists(t, filepath.Join(dir, "orphan.bin"))
			require.Equal(t, uint64(7), progress.BytesReceived())
			require.Equal(t, uint64(7), progress.BytesTotal())
			require.Equal(t, uint64(1), progress.FilesCompleted())
			require.Equal(t, uint64(1), progress.FilesTotal())
		})
	}
}
