package grpc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// SnapshotDonor exposes the applier state needed by the snapshot service.
// SnapshotReadiness must not wait or create a checkpoint.
type SnapshotDonor interface {
	WaitForCheckpointable(ctx context.Context, targetIndex uint64) error
	SnapshotReadiness() (lastPersistedIndex uint64, resynchronizing bool)
}

// SnapshotServiceServerImpl implements the SnapshotService gRPC server.
type SnapshotServiceServerImpl struct {
	snapshotpb.UnimplementedSnapshotServiceServer

	logger       logging.Logger
	store        *dal.Store
	sessions     *snapshotSessionStore
	nextSyncID   atomic.Uint64
	donor        SnapshotDonor
	prepareMu    sync.Mutex
	prepareSlots chan struct{}
	preparations map[string]*snapshotPreparation
}

type snapshotPreparation struct {
	done chan struct{}
	resp *snapshotpb.PrepareSnapshotResponse
	err  error
}

// NewSnapshotServiceServer creates a new SnapshotServiceServer.
func NewSnapshotServiceServer(logger logging.Logger, s *dal.Store, sessionTTL time.Duration, donor SnapshotDonor) snapshotpb.SnapshotServiceServer {
	l := logger.WithField("component", "snapshot-server")

	if sessionTTL == 0 {
		sessionTTL = defaultSessionTTL
	}

	return &SnapshotServiceServerImpl{
		logger:       l,
		store:        s,
		sessions:     newSnapshotSessionStore(s, l, sessionTTL),
		donor:        donor,
		prepareSlots: make(chan struct{}, 1),
		preparations: make(map[string]*snapshotPreparation),
	}
}

// CheckSnapshotReadiness reports whether a checkpoint can be served without
// creating one. The result is advisory: PrepareSnapshot repeats the durable
// readiness check because the state may change between the two calls.
func (s *SnapshotServiceServerImpl) CheckSnapshotReadiness(_ context.Context, req *snapshotpb.CheckSnapshotReadinessRequest) (*snapshotpb.CheckSnapshotReadinessResponse, error) {
	lastPersistedIndex, resynchronizing := s.donor.SnapshotReadiness()

	return &snapshotpb.CheckSnapshotReadinessResponse{
		Ready:              !resynchronizing && lastPersistedIndex >= req.GetMinAppliedIndex(),
		LastPersistedIndex: lastPersistedIndex,
		Resynchronizing:    resynchronizing,
	}, nil
}

// StartSnapshotService starts the session reaper. Keeping this acquisition in
// Fx OnStart ensures a failed earlier startup hook cannot leak the goroutine.
func StartSnapshotService(server snapshotpb.SnapshotServiceServer) {
	if impl, ok := server.(*SnapshotServiceServerImpl); ok {
		impl.sessions.start()
	}
}

// StopSnapshotService stops the session reaper and retires all sessions.
// Checkpoints still in use are removed when their last FetchFile completes.
func StopSnapshotService(server snapshotpb.SnapshotServiceServer) {
	if impl, ok := server.(*SnapshotServiceServerImpl); ok {
		impl.sessions.stop()
	}
}

// PrepareSnapshot creates a fresh Pebble checkpoint, builds a manifest, and
// returns a session ID that can be used for parallel FetchFile calls.
func (s *SnapshotServiceServerImpl) PrepareSnapshot(ctx context.Context, req *snapshotpb.PrepareSnapshotRequest) (*snapshotpb.PrepareSnapshotResponse, error) {
	overallStart := time.Now()

	s.logger.WithFields(map[string]any{
		"node_id":         req.GetNodeId(),
		"minAppliedIndex": req.GetMinAppliedIndex(),
	}).Infof("PrepareSnapshot request received")

	// Wait until the FSM has applied at least the requested index before
	// creating the Pebble checkpoint. Without this, the checkpoint could be
	// taken before the FSM commits entries the follower needs, causing the
	// follower to restore a state behind its Raft snapshot index. A store
	// that is itself resynchronizing is refused outright: the requester
	// treats FailedPrecondition as final and moves on to another donor
	// instead of waiting out its PrepareSnapshot budget here.
	minAppliedIndex := req.GetMinAppliedIndex()
	waitStart := time.Now()
	if err := s.donor.WaitForCheckpointable(ctx, minAppliedIndex); err != nil {
		if errors.Is(err, state.ErrStoreResynchronizing) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}

		return nil, fmt.Errorf("waiting for FSM to apply index %d: %w", minAppliedIndex, err)
	}

	if d := time.Since(waitStart); d > 100*time.Millisecond {
		s.logger.WithFields(map[string]any{
			"minAppliedIndex": minAppliedIndex,
			"duration":        d.String(),
		}).Infof("FSM caught up to minAppliedIndex for PrepareSnapshot")
	}

	nodeID := req.GetNodeId()
	var prep *snapshotPreparation
	for {
		s.prepareMu.Lock()
		if s.prepareSlots == nil {
			s.prepareSlots = make(chan struct{}, 1)
		}
		if s.preparations == nil {
			s.preparations = make(map[string]*snapshotPreparation)
		}
		if nodeID != "" {
			if existing, ok := s.preparations[nodeID]; ok {
				if existing.minAppliedIndex >= minAppliedIndex {
					s.prepareMu.Unlock()
					select {
					case <-existing.done:
						if errors.Is(existing.err, context.Canceled) || errors.Is(existing.err, context.DeadlineExceeded) {
							if ctx.Err() != nil {
								return nil, ctx.Err()
							}
							continue
						}
						return existing.resp, existing.err
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				done := existing.done
				s.prepareMu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		prep = &snapshotPreparation{done: make(chan struct{}), minAppliedIndex: minAppliedIndex}
		if nodeID != "" {
			s.preparations[nodeID] = prep
		}
		s.prepareMu.Unlock()
		defer func() {
			s.prepareMu.Lock()
			if nodeID != "" && s.preparations[nodeID] == prep {
				delete(s.preparations, nodeID)
			}
			close(prep.done)
			s.prepareMu.Unlock()
		}()
		select {
		case s.prepareSlots <- struct{}{}:
			defer func() { <-s.prepareSlots }()
		case <-ctx.Done():
			prep.err = ctx.Err()
			return nil, prep.err
		}
		break
	}

	syncName := "follower-sync-" + strconv.FormatUint(s.nextSyncID.Add(1), 10)

	checkpointStart := time.Now()
	checkpointPath, err := s.store.CreateTemporaryCheckpoint(syncName)
	if err != nil {
		prep.err = fmt.Errorf("creating temporary checkpoint: %w", err)

		return nil, prep.err
	}

	s.logger.WithFields(map[string]any{
		"syncName": syncName,
		"duration": time.Since(checkpointStart).String(),
	}).Infof("Temporary checkpoint created for follower sync")

	manifestStart := time.Now()
	manifest, err := buildManifest(context.WithoutCancel(ctx), checkpointPath)
	if err != nil {
		// Clean up checkpoint on manifest build failure.
		if rmErr := s.store.RemoveTemporaryCheckpoint(syncName); rmErr != nil {
			s.logger.Errorf("Failed to remove checkpoint after manifest error: %v", rmErr)
		}

		prep.err = fmt.Errorf("building manifest: %w", err)

		return nil, prep.err
	}

	sessionID, err := s.sessions.create(syncName, checkpointPath)
	if err != nil {
		if rmErr := s.store.RemoveTemporaryCheckpoint(syncName); rmErr != nil {
			s.logger.Errorf("Failed to remove checkpoint after session creation error: %v", rmErr)
		}

		prep.err = fmt.Errorf("creating session: %w", err)

		return nil, prep.err
	}

	s.logger.WithFields(map[string]any{
		"sessionId":        sessionID,
		"files":            len(manifest.GetFiles()),
		"manifestDuration": time.Since(manifestStart).String(),
		"totalDuration":    time.Since(overallStart).String(),
	}).Infof("Snapshot session created")

	prep.resp = &snapshotpb.PrepareSnapshotResponse{
		SessionId: sessionID,
		Manifest:  manifest,
	}

	return prep.resp, nil
}

// FetchFile streams a single file from a prepared snapshot session.
func (s *SnapshotServiceServerImpl) FetchFile(req *snapshotpb.FetchFileRequest, stream ggrpc.ServerStreamingServer[snapshotpb.FetchFileResponse]) error {
	session, ok := s.sessions.acquire(req.GetSessionId())
	if !ok {
		return status.Errorf(codes.NotFound, "session %s not found or expired", req.GetSessionId())
	}
	defer s.sessions.release(session)

	// Validate the path stays within the checkpoint directory.
	relPath := req.GetPath()
	if !filepath.IsLocal(relPath) {
		return status.Errorf(codes.InvalidArgument, "invalid file path: %s", relPath)
	}

	buf := make([]byte, defaultChunkSize)

	return streamOneFile(session.checkpointPath, relPath, buf, func(resp *snapshotpb.FetchFileResponse) error {
		return stream.Send(resp)
	})
}

// CloseSession atomically retires the snapshot session, preventing new
// FetchFile acquisitions. Already-acquired FetchFile calls may finish; the
// final release removes the temporary checkpoint. The operation is idempotent
// and does not wait for active calls to finish.
func (s *SnapshotServiceServerImpl) CloseSession(_ context.Context, req *snapshotpb.CloseSessionRequest) (*snapshotpb.CloseSessionResponse, error) {
	s.sessions.remove(req.GetSessionId())

	if s.logger.Enabled(logging.TraceLevel) {
		s.logger.WithFields(map[string]any{
			"sessionId": req.GetSessionId(),
		}).Tracef("Session closed")
	}

	return &snapshotpb.CloseSessionResponse{}, nil
}

// RegisterSnapshotService registers the SnapshotService on a gRPC service registrar.
func RegisterSnapshotService(registrar ggrpc.ServiceRegistrar, snapshotServiceServer snapshotpb.SnapshotServiceServer) {
	snapshotpb.RegisterSnapshotServiceServer(registrar, snapshotServiceServer)
}
