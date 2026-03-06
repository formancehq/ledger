package ctrl

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/tarutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
)

// grpcSnapshotFetcher implements raft.SnapshotFetcher using gRPC.
type grpcSnapshotFetcher struct {
	client snapshotpb.SnapshotServiceClient
}

// isUnavailableError checks if the error is a gRPC Unavailable error (connection refused, etc.)
func isUnavailableError(err error) bool {
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.Unavailable
	}

	return false
}

func (f *grpcSnapshotFetcher) FetchSnapshot(ctx context.Context, snapshotID uint64, targetDir string, progress *state.SyncProgress) (uint64, string, error) {
	// Fetch total size upfront so callers can display progress.
	if progress != nil {
		desc, err := f.client.DescribeSnapshot(ctx, &snapshotpb.DescribeSnapshotRequest{
			SnapshotId: snapshotID,
		})
		if err == nil && desc.GetStatus() == snapshotpb.DescribeSnapshotResponse_READY {
			progress.SetTotal(desc.GetContentSize())
		}
	}

	// Request the snapshot stream
	stream, err := f.client.FetchSnapshot(ctx, &snapshotpb.FetchSnapshotRequest{
		SnapshotId: snapshotID,
	})
	if err != nil {
		if isUnavailableError(err) {
			return 0, "", fmt.Errorf("starting snapshot fetch: %w", state.ErrNotAvailable)
		}

		return 0, "", fmt.Errorf("starting snapshot fetch: %w", err)
	}

	// Create a pipe to stream tar data
	pr, pw := io.Pipe()

	// Channel to collect errors from the tar extraction goroutine
	extractErrCh := make(chan error, 1)

	// Start tar extraction in a goroutine
	go func() {
		defer func() {
			_ = pr.Close()
		}()

		extractErrCh <- tarutil.ExtractTar(pr, targetDir)
	}()

	// Receive chunks from the stream and write to the pipe
	var (
		totalSize   uint64
		contentHash string
	)

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			_ = pw.CloseWithError(err)
			if isUnavailableError(err) {
				return 0, "", fmt.Errorf("receiving snapshot chunk: %w", state.ErrNotAvailable)
			}

			return 0, "", fmt.Errorf("receiving snapshot chunk: %w", err)
		}

		if len(resp.GetData()) > 0 {
			if _, err := pw.Write(resp.GetData()); err != nil {
				return 0, "", fmt.Errorf("writing to tar pipe: %w", err)
			}

			if progress != nil {
				progress.AddReceived(uint64(len(resp.GetData())))
			}
		}

		if resp.GetEof() {
			totalSize = resp.GetContentSize()
			contentHash = resp.GetContentSha256()

			break
		}
	}

	// Close the write end of the pipe to signal EOF to tar reader
	if err := pw.Close(); err != nil {
		return 0, "", fmt.Errorf("closing pipe: %w", err)
	}

	// Wait for extraction to complete
	if err := <-extractErrCh; err != nil {
		return 0, "", fmt.Errorf("extracting tar: %w", err)
	}

	return totalSize, contentHash, nil
}

// grpcSnapshotFetcherProvider provides snapshot fetchers for peers.
type grpcSnapshotFetcherProvider struct {
	transport *node.DefaultTransport
}

func (p *grpcSnapshotFetcherProvider) GetForPeer(id uint64) (state.SnapshotFetcher, error) {
	conn := p.transport.GetPeerConnection(id)

	return &grpcSnapshotFetcher{
		client: snapshotpb.NewSnapshotServiceClient(conn),
	}, nil
}

// GRPCSnapshotFetcherProvider creates a new snapshot fetcher provider using gRPC.
func GRPCSnapshotFetcherProvider(transport *node.DefaultTransport) state.SnapshotFetcherProvider {
	return &grpcSnapshotFetcherProvider{
		transport: transport,
	}
}
