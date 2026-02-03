package application

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/grpc"
)

const (
	// defaultChunkSize is the default size of each chunk sent in FetchSnapshot stream (64KB)
	defaultChunkSize = 64 * 1024
)

// SnapshotServiceServerImpl implements the SnapshotService gRPC server.
type SnapshotServiceServerImpl struct {
	snapshotpb.UnimplementedSnapshotServiceServer
	logger logging.Logger
	store  *data.Store
}

// NewSnapshotServiceServer creates a new SnapshotServiceServer.
func NewSnapshotServiceServer(logger logging.Logger, s *data.Store) snapshotpb.SnapshotServiceServer {
	return &SnapshotServiceServerImpl{
		logger: logger.WithField("component", "snapshot-server"),
		store:  s,
	}
}

// DescribeSnapshot returns metadata about a snapshot.
func (s *SnapshotServiceServerImpl) DescribeSnapshot(ctx context.Context, req *snapshotpb.DescribeSnapshotRequest) (*snapshotpb.DescribeSnapshotResponse, error) {
	s.logger.WithFields(map[string]any{
		"snapshot_id": req.SnapshotId,
		"node_id":     req.NodeId,
	}).Debugf("DescribeSnapshot request received")

	// Get checkpoint path
	checkpointPath, err := s.store.GetCheckpointPath(req.SnapshotId)
	if err != nil {
		return &snapshotpb.DescribeSnapshotResponse{
			SnapshotId: req.SnapshotId,
			Status:     snapshotpb.DescribeSnapshotResponse_NOT_FOUND,
		}, nil
	}

	// Calculate total size of the checkpoint directory
	var totalSize uint64
	err = filepath.Walk(checkpointPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += uint64(info.Size())
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("calculating checkpoint size: %w", err)
	}

	return &snapshotpb.DescribeSnapshotResponse{
		SnapshotId:  req.SnapshotId,
		ContentSize: totalSize,
		Format:      snapshotpb.DescribeSnapshotResponse_FORMAT_TAR,
		Compression: snapshotpb.DescribeSnapshotResponse_COMP_NONE,
		Status:      snapshotpb.DescribeSnapshotResponse_READY,
	}, nil
}

// FetchSnapshot streams the snapshot as a tar archive.
func (s *SnapshotServiceServerImpl) FetchSnapshot(req *snapshotpb.FetchSnapshotRequest, stream grpc.ServerStreamingServer[snapshotpb.FetchSnapshotResponse]) error {
	s.logger.WithFields(map[string]any{
		"snapshot_id": req.SnapshotId,
		"offset":      req.Offset,
		"node_id":     req.NodeId,
	}).Debugf("FetchSnapshot request received")

	// Get checkpoint path
	checkpointPath, err := s.store.GetCheckpointPath(req.SnapshotId)
	if err != nil {
		return fmt.Errorf("checkpoint not found: %w", err)
	}

	// Create a pipe to stream tar data
	pr, pw := io.Pipe()

	// Hash writer to calculate SHA256
	hash := sha256.New()

	// Channel to collect errors from the tar writer goroutine
	errCh := make(chan error, 1)

	// Start tar writer in a goroutine
	go func() {
		defer func() {
			_ = pw.Close()
		}()

		tw := tar.NewWriter(io.MultiWriter(pw, hash))
		defer func() {
			_ = tw.Close()
		}()

		err := filepath.Walk(checkpointPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Get relative path
			relPath, err := filepath.Rel(checkpointPath, path)
			if err != nil {
				return err
			}

			// Create tar header
			header, err := tar.FileInfoHeader(info, "")
			if err != nil {
				return err
			}
			header.Name = relPath

			if err := tw.WriteHeader(header); err != nil {
				return err
			}

			// Write file content if not a directory
			if !info.IsDir() {
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer func() {
					_ = f.Close()
				}()

				if _, err := io.Copy(tw, f); err != nil {
					return err
				}
			}

			return nil
		})

		errCh <- err
	}()

	// Stream data in chunks
	var (
		offset      uint64
		totalSize   uint64
		headerSent  bool
		contentHash string
	)

	buf := make([]byte, defaultChunkSize)
	for {
		n, err := pr.Read(buf)
		if n > 0 {
			// Skip bytes if we're resuming from an offset
			if offset < req.Offset {
				skip := req.Offset - offset
				if skip >= uint64(n) {
					offset += uint64(n)
					continue
				}
				buf = buf[skip:]
				n -= int(skip)
				offset = req.Offset
			}

			resp := &snapshotpb.FetchSnapshotResponse{
				Header:      !headerSent,
				SnapshotId:  req.SnapshotId,
				ChunkOffset: offset,
				Data:        buf[:n],
				Eof:         false,
			}
			headerSent = true

			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("sending chunk: %w", err)
			}

			offset += uint64(n)
			totalSize += uint64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading tar data: %w", err)
		}
	}

	// Wait for tar writer to finish and check for errors
	if err := <-errCh; err != nil {
		return fmt.Errorf("creating tar archive: %w", err)
	}

	// Calculate final hash
	contentHash = hex.EncodeToString(hash.Sum(nil))

	// Send final chunk with EOF
	finalResp := &snapshotpb.FetchSnapshotResponse{
		Header:        !headerSent,
		SnapshotId:    req.SnapshotId,
		ContentSha256: contentHash,
		ContentSize:   totalSize,
		ChunkOffset:   offset,
		Eof:           true,
	}

	if err := stream.Send(finalResp); err != nil {
		return fmt.Errorf("sending final chunk: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"snapshot_id":  req.SnapshotId,
		"total_size":   totalSize,
		"content_hash": contentHash,
	}).Infof("FetchSnapshot completed")

	return nil
}

// RegisterSnapshotService registers the SnapshotService with a gRPC server.
func RegisterSnapshotService(server *grpc.Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(server, snapshotServiceServer)
}
