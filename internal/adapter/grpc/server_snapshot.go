package grpc

import (
	"fmt"
	"strconv"
	"sync/atomic"

	ggrpc "google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// SnapshotServiceServerImpl implements the SnapshotService gRPC server.
type SnapshotServiceServerImpl struct {
	snapshotpb.UnimplementedSnapshotServiceServer

	logger     logging.Logger
	store      *dal.Store
	nextSyncID atomic.Uint64
}

// NewSnapshotServiceServer creates a new SnapshotServiceServer.
func NewSnapshotServiceServer(logger logging.Logger, s *dal.Store) snapshotpb.SnapshotServiceServer {
	return &SnapshotServiceServerImpl{
		logger: logger.WithField("component", "snapshot-server"),
		store:  s,
	}
}

// FetchSnapshot creates a fresh Pebble checkpoint and streams it as a tar archive.
// Uses a temporary checkpoint to avoid interference with cleanupOldCheckpoints
// running in the background goroutine (which only deletes numbered checkpoints).
func (s *SnapshotServiceServerImpl) FetchSnapshot(req *snapshotpb.FetchSnapshotRequest, stream ggrpc.ServerStreamingServer[snapshotpb.FetchSnapshotResponse]) error {
	if s.logger.Enabled(logging.DebugLevel) {
		s.logger.WithFields(map[string]any{
			"node_id": req.GetNodeId(),
		}).Debugf("FetchSnapshot request received")
	}

	// Use a temporary checkpoint with a unique name per call.
	// Temporary checkpoints live in tmp/ and are not affected by
	// cleanupOldCheckpoints (which only touches checkpoints/{N}/).
	syncName := "follower-sync-" + strconv.FormatUint(s.nextSyncID.Add(1), 10)

	checkpointPath, err := s.store.CreateTemporaryCheckpoint(syncName)
	if err != nil {
		return fmt.Errorf("creating temporary checkpoint: %w", err)
	}

	defer func() {
		if rmErr := s.store.RemoveTemporaryCheckpoint(syncName); rmErr != nil {
			s.logger.WithFields(map[string]any{
				"error": rmErr,
				"name":  syncName,
			}).Errorf("Failed to remove temporary checkpoint after streaming")
		}
	}()

	s.logger.WithFields(map[string]any{
		"name": syncName,
	}).Infof("Temporary checkpoint created for follower sync")

	err = StreamDirAsTar(checkpointPath, 0, func(chunk TarStreamChunk) error {
		return stream.Send(&snapshotpb.FetchSnapshotResponse{
			Header:        chunk.IsFirst,
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

	if s.logger.Enabled(logging.DebugLevel) {
		s.logger.WithFields(map[string]any{
			"name": syncName,
		}).Debugf("FetchSnapshot completed")
	}

	return nil
}

// RegisterSnapshotService registers the SnapshotService with a gRPC server.
func RegisterSnapshotService(server *ggrpc.Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(server, snapshotServiceServer)
}
