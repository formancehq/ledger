package service

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
)

// grpcSnapshotFetcher implements raft.SnapshotFetcher using gRPC.
type grpcSnapshotFetcher struct {
	client snapshotpb.SnapshotServiceClient
}

func (f *grpcSnapshotFetcher) FetchSnapshot(ctx context.Context, snapshotID uint64, targetDir string) (uint64, string, error) {
	// Request the snapshot stream
	stream, err := f.client.FetchSnapshot(ctx, &snapshotpb.FetchSnapshotRequest{
		SnapshotId: snapshotID,
	})
	if err != nil {
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

		tr := tar.NewReader(pr)
		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				extractErrCh <- fmt.Errorf("reading tar header: %w", err)
				return
			}

			targetPath := filepath.Join(targetDir, header.Name)

			switch header.Typeflag {
			case tar.TypeDir:
				if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
					extractErrCh <- fmt.Errorf("creating directory %s: %w", targetPath, err)
					return
				}
			case tar.TypeReg:
				// Ensure parent directory exists
				if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
					extractErrCh <- fmt.Errorf("creating parent directory for %s: %w", targetPath, err)
					return
				}

				f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
				if err != nil {
					extractErrCh <- fmt.Errorf("creating file %s: %w", targetPath, err)
					return
				}

				if _, err := io.Copy(f, tr); err != nil {
					_ = f.Close()
					extractErrCh <- fmt.Errorf("writing file %s: %w", targetPath, err)
					return
				}

				if err := f.Close(); err != nil {
					extractErrCh <- fmt.Errorf("closing file %s: %w", targetPath, err)
					return
				}
			}
		}

		extractErrCh <- nil
	}()

	// Receive chunks from the stream and write to the pipe
	var (
		totalSize   uint64
		contentHash string
	)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = pw.CloseWithError(err)
			return 0, "", fmt.Errorf("receiving snapshot chunk: %w", err)
		}

		if len(resp.Data) > 0 {
			if _, err := pw.Write(resp.Data); err != nil {
				return 0, "", fmt.Errorf("writing to tar pipe: %w", err)
			}
		}

		if resp.Eof {
			totalSize = resp.ContentSize
			contentHash = resp.ContentSha256
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
	transport *raft.DefaultTransport
}

func (p *grpcSnapshotFetcherProvider) GetForPeer(id uint64) (raft.SnapshotFetcher, error) {
	conn := p.transport.GetPeerConnection(id)
	return &grpcSnapshotFetcher{
		client: snapshotpb.NewSnapshotServiceClient(conn),
	}, nil
}

// GRPCSnapshotFetcherProvider creates a new snapshot fetcher provider using gRPC.
func GRPCSnapshotFetcherProvider(transport *raft.DefaultTransport) raft.SnapshotFetcherProvider {
	return &grpcSnapshotFetcherProvider{
		transport: transport,
	}
}
