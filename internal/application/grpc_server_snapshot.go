package application

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/grpc"
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

	err = StreamDirAsTar(checkpointPath, req.Offset, func(chunk TarStreamChunk) error {
		return stream.Send(&snapshotpb.FetchSnapshotResponse{
			Header:        chunk.IsFirst,
			SnapshotId:    req.SnapshotId,
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
		"snapshot_id": req.SnapshotId,
	}).Infof("FetchSnapshot completed")

	return nil
}

// RegisterSnapshotService registers the SnapshotService with a gRPC server.
func RegisterSnapshotService(server *grpc.Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(server, snapshotServiceServer)
}
