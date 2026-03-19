package grpc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	ggrpc "google.golang.org/grpc"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// SnapshotServiceServerImpl implements the SnapshotService gRPC server.
type SnapshotServiceServerImpl struct {
	snapshotpb.UnimplementedSnapshotServiceServer

	logger logging.Logger
	store  *dal.Store
}

// NewSnapshotServiceServer creates a new SnapshotServiceServer.
func NewSnapshotServiceServer(logger logging.Logger, s *dal.Store) snapshotpb.SnapshotServiceServer {
	return &SnapshotServiceServerImpl{
		logger: logger.WithField("component", "snapshot-server"),
		store:  s,
	}
}

// DescribeSnapshot returns metadata about a snapshot.
func (s *SnapshotServiceServerImpl) DescribeSnapshot(ctx context.Context, req *snapshotpb.DescribeSnapshotRequest) (*snapshotpb.DescribeSnapshotResponse, error) {
	s.logger.WithFields(map[string]any{
		"snapshot_id": req.GetSnapshotId(),
		"node_id":     req.GetNodeId(),
	}).Debugf("DescribeSnapshot request received")

	// Get checkpoint path
	checkpointPath, err := s.store.GetCheckpointPath(req.GetSnapshotId())
	if err != nil {
		return &snapshotpb.DescribeSnapshotResponse{
			SnapshotId: req.GetSnapshotId(),
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
		SnapshotId:  req.GetSnapshotId(),
		ContentSize: totalSize,
		Format:      snapshotpb.DescribeSnapshotResponse_FORMAT_TAR,
		Compression: snapshotpb.DescribeSnapshotResponse_COMP_NONE,
		Status:      snapshotpb.DescribeSnapshotResponse_READY,
	}, nil
}

// FetchSnapshot streams the snapshot as a tar archive.
func (s *SnapshotServiceServerImpl) FetchSnapshot(req *snapshotpb.FetchSnapshotRequest, stream ggrpc.ServerStreamingServer[snapshotpb.FetchSnapshotResponse]) error {
	s.logger.WithFields(map[string]any{
		"snapshot_id": req.GetSnapshotId(),
		"offset":      req.GetOffset(),
		"node_id":     req.GetNodeId(),
	}).Debugf("FetchSnapshot request received")

	// Get checkpoint path
	checkpointPath, err := s.store.GetCheckpointPath(req.GetSnapshotId())
	if err != nil {
		return fmt.Errorf("checkpoint not found: %w", err)
	}

	err = StreamDirAsTar(checkpointPath, req.GetOffset(), func(chunk TarStreamChunk) error {
		return stream.Send(&snapshotpb.FetchSnapshotResponse{
			Header:        chunk.IsFirst,
			SnapshotId:    req.GetSnapshotId(),
			ChunkOffset:   chunk.ChunkOffset,
			Data:          chunk.Data,
			Eof:           chunk.IsEOF,
			ContentSha256: chunk.ContentSHA256,
			ContentSize:   chunk.ContentSize,
		})
	})
	if err != nil {
		return fmt.Errorf("streaming snapshot: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"snapshot_id": req.GetSnapshotId(),
	}).Infof("FetchSnapshot completed")

	return nil
}

// RegisterSnapshotService registers the SnapshotService with a gRPC server.
func RegisterSnapshotService(server *ggrpc.Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(server, snapshotServiceServer)
}
