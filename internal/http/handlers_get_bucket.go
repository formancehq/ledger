package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleGetBucket handles GET /buckets/{bucketName} to get a bucket with its Raft state
func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.BadRequest(w, "bucket name is required", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	bucket, err := s.cluster.GetBucketWithRaftState(bucketName)
	if err != nil {
		s.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Errorf("Failed to get bucket")
		api.InternalServerError(w, r, err)
		return
	}

	if bucket == nil {
		api.NotFound(w, errors.New("bucket not found"))
		return
	}

	api.Ok(w, bucket)
}

