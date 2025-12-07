package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// handleCreateBucket handles POST /buckets/{bucketName} to create a new bucket
func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucketName")
	if bucketName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("bucket name is required"))
		return
	}

	// Parse request body (driver and config are required)
	var req struct {
		Driver string                 `json:"driver"`
		Config map[string]interface{} `json:"config"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Validate required fields
	if req.Driver == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("driver is required"))
		return
	}

	if req.Config == nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("config is required"))
		return
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(req.Driver, req.Config); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_CONFIG", err)
		return
	}

	// Check if bucket already exists (validation en amont)
	if _, exists := s.cluster.GetBucket(bucketName); exists {
		api.WriteErrorResponse(w, http.StatusConflict, "BUCKET_ALREADY_EXISTS", fmt.Errorf("bucket %s already exists", bucketName))
		return
	}

	// Create bucket via cluster
	if err := s.cluster.CreateBucket(bucketName, req.Driver, req.Config); err != nil {
		s.logger.Error("Failed to create bucket", zap.String("name", bucketName), zap.Error(err))
		api.InternalServerError(w, r, err)
		return
	}

	// Get the created bucket to return it
	bucket, exists := s.cluster.GetBucket(bucketName)
	if !exists {
		s.logger.Warn("Failed to retrieve created bucket", zap.String("name", bucketName))
		// Still return success since creation succeeded
		api.Created(w, service.BucketInfo{
			Name:   bucketName,
			Driver: req.Driver,
			Config: req.Config,
		})
		return
	}

	// Return the bucket info
	api.Created(w, bucket)
}

