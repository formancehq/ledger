package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleCreateBucketSnapshot handles POST /buckets/{bucketName}/snapshot to create a snapshot for a bucket
func (s *Server) handleCreateBucketSnapshot(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	bucket, err := s.cluster.GetBucket(r.Context(), bucketName)
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	if err := bucket.Snapshot(r.Context()); err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Return success response
	api.Ok(w, map[string]interface{}{
		"message": "Snapshot created successfully.",
	})
}
