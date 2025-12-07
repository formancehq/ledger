package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleDeleteBucket handles DELETE /buckets/{bucketName} to delete a bucket
func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Delete bucket via cluster
	if err := s.cluster.DeleteBucket(bucketName); err != nil {
		s.logger.WithFields(map[string]any{"name": bucketName, "error": err}).Errorf("Failed to delete bucket")

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
		"message": fmt.Sprintf("Bucket %s deleted successfully", bucketName),
	})
}
