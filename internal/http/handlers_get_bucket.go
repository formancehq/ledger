package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleGetBucket handles GET /buckets/{bucketName} to get bucket information
func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.BadRequest(w, "bucket name is required", errors.New("bucket name is required"))
		return
	}

	bucket, err := s.cluster.GetBucketInfo(r.Context(), bucketName)
	if err != nil {
		s.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Errorf("Failed to get bucket")
		handleError(w, r, err)
		return
	}

	api.Ok(w, bucket)
}
