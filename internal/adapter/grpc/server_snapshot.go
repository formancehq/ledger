package grpc

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// SnapshotServiceServerImpl implements the SnapshotService gRPC server.
type SnapshotServiceServerImpl struct {
	snapshotpb.UnimplementedSnapshotServiceServer

	logger     logging.Logger
	store      *dal.Store
	sessions   *snapshotSessionStore
	nextSyncID atomic.Uint64
}

// NewSnapshotServiceServer creates a new SnapshotServiceServer.
func NewSnapshotServiceServer(logger logging.Logger, s *dal.Store, sessionTTL time.Duration) snapshotpb.SnapshotServiceServer {
	l := logger.WithField("component", "snapshot-server")

	if sessionTTL == 0 {
		sessionTTL = defaultSessionTTL
	}

	return &SnapshotServiceServerImpl{
		logger:   l,
		store:    s,
		sessions: newSnapshotSessionStore(s, l, sessionTTL),
	}
}

// StopSnapshotService stops the session reaper and cleans up all active sessions.
func StopSnapshotService(server snapshotpb.SnapshotServiceServer) {
	if impl, ok := server.(*SnapshotServiceServerImpl); ok {
		impl.sessions.stop()
	}
}

// PrepareSnapshot creates a fresh Pebble checkpoint, builds a manifest, and
// returns a session ID that can be used for parallel FetchFile calls.
func (s *SnapshotServiceServerImpl) PrepareSnapshot(_ context.Context, req *snapshotpb.PrepareSnapshotRequest) (*snapshotpb.PrepareSnapshotResponse, error) {
	if s.logger.Enabled(logging.DebugLevel) {
		s.logger.WithFields(map[string]any{
			"node_id": req.GetNodeId(),
		}).Debugf("PrepareSnapshot request received")
	}

	syncName := "follower-sync-" + strconv.FormatUint(s.nextSyncID.Add(1), 10)

	checkpointPath, err := s.store.CreateTemporaryCheckpoint(syncName)
	if err != nil {
		return nil, fmt.Errorf("creating temporary checkpoint: %w", err)
	}

	manifest, err := buildManifest(checkpointPath)
	if err != nil {
		// Clean up checkpoint on manifest build failure.
		if rmErr := s.store.RemoveTemporaryCheckpoint(syncName); rmErr != nil {
			s.logger.Errorf("Failed to remove checkpoint after manifest error: %v", rmErr)
		}

		return nil, fmt.Errorf("building manifest: %w", err)
	}

	sessionID, err := s.sessions.create(syncName, checkpointPath)
	if err != nil {
		if rmErr := s.store.RemoveTemporaryCheckpoint(syncName); rmErr != nil {
			s.logger.Errorf("Failed to remove checkpoint after session creation error: %v", rmErr)
		}

		return nil, fmt.Errorf("creating session: %w", err)
	}

	s.logger.WithFields(map[string]any{
		"sessionId": sessionID,
		"files":     len(manifest.GetFiles()),
	}).Infof("Snapshot session created")

	return &snapshotpb.PrepareSnapshotResponse{
		SessionId: sessionID,
		Manifest:  manifest,
	}, nil
}

// FetchFile streams a single file from a prepared snapshot session.
func (s *SnapshotServiceServerImpl) FetchFile(req *snapshotpb.FetchFileRequest, stream ggrpc.ServerStreamingServer[snapshotpb.FetchFileResponse]) error {
	session, ok := s.sessions.get(req.GetSessionId())
	if !ok {
		return status.Errorf(codes.NotFound, "session %s not found or expired", req.GetSessionId())
	}

	// Validate the path stays within the checkpoint directory.
	relPath := req.GetPath()
	fullPath := filepath.Join(session.checkpointPath, relPath)

	resolved, err := filepath.Rel(session.checkpointPath, fullPath)
	if err != nil || resolved != relPath {
		return status.Errorf(codes.InvalidArgument, "invalid file path: %s", relPath)
	}

	buf := make([]byte, defaultChunkSize)

	return streamOneFile(session.checkpointPath, relPath, buf, func(resp *snapshotpb.FetchFileResponse) error {
		return stream.Send(resp)
	})
}

// CloseSession releases the snapshot session and its temporary checkpoint.
func (s *SnapshotServiceServerImpl) CloseSession(_ context.Context, req *snapshotpb.CloseSessionRequest) (*snapshotpb.CloseSessionResponse, error) {
	s.sessions.remove(req.GetSessionId())

	if s.logger.Enabled(logging.DebugLevel) {
		s.logger.WithFields(map[string]any{
			"sessionId": req.GetSessionId(),
		}).Debugf("Session closed")
	}

	return &snapshotpb.CloseSessionResponse{}, nil
}

// RegisterSnapshotService registers the SnapshotService with a gRPC server.
func RegisterSnapshotService(server *ggrpc.Server, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(server, snapshotServiceServer)
}
