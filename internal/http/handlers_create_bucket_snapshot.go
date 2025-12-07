package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// handleCreateBucketSnapshot handles POST /buckets/{bucketName}/snapshot to create a snapshot for a bucket
func (s *Server) handleCreateBucketSnapshot(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get bucket Raft state to check if we are the leader of this bucket's Raft group
	bucketWithState, err := s.cluster.GetBucketWithRaftState(bucketName)
	if err != nil {
		s.logger.Error("Failed to get bucket Raft state", zap.String("bucket", bucketName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	if bucketWithState == nil {
		api.WriteErrorResponse(w, http.StatusNotFound, "BUCKET_NOT_FOUND", errors.New("bucket not found"))
		return
	}

	// Check if we are the leader of this bucket's Raft group
	isBucketLeader := bucketWithState.RaftState != nil && bucketWithState.RaftState.State == "Leader"

	if isBucketLeader {
		// We are the leader of this bucket's Raft group, call directly
		if err := s.cluster.CreateBucketSnapshot(bucketName); err != nil {
			s.logger.Error("Failed to create bucket snapshot", zap.String("bucket", bucketName), zap.Error(err))

			// Check if bucket does not exist
			if err.Error() == fmt.Sprintf("bucket does not exist: %s", bucketName) {
				api.WriteErrorResponse(w, http.StatusNotFound, "BUCKET_NOT_FOUND", err)
				return
			}

			api.InternalServerError(w, r, err)
			return
		}

		// Return success response
		api.Ok(w, map[string]interface{}{
			"message": fmt.Sprintf("Snapshot created successfully for bucket %s", bucketName),
		})
		return
	}

	// We are not the leader of this bucket's Raft group, forward via gRPC
	// The gRPC server will route to the leader of the bucket's Raft group
	client := s.cluster.GetLeaderGRPCClient()
	if client == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("not connected to leader gRPC server"))
		return
	}

	// Call leader via gRPC (will route to the leader of the bucket's Raft group)
	resp, err := client.CreateBucketSnapshot(r.Context(), &service.CreateBucketSnapshotRequest{
		BucketName: bucketName,
	})
	if err != nil {
		s.logger.Error("Failed to create bucket snapshot via gRPC", zap.String("bucket", bucketName), zap.Error(err))
		api.WriteErrorResponse(w, http.StatusInternalServerError, "SNAPSHOT_FAILED", err)
		return
	}

	// Return success response
	api.Ok(w, map[string]interface{}{
		"message": resp.Message,
	})
}

