package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/go-chi/chi/v5"
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

	// Config is optional for SQLite (paths are auto-generated), but required for other drivers
	if req.Config == nil {
		if req.Driver != "sqlite" {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("config is required"))
			return
		}
		// For SQLite, use empty config
		req.Config = make(map[string]interface{})
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(req.Driver, req.Config); err != nil {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_CONFIG", err)
		return
	}

	// Create bucket via cluster
	bucket, err := s.cluster.CreateBucket(r.Context(), bucketName, req.Driver, req.Config)
	if err != nil {
		api.InternalServerError(w, r, err)
		return
	}

	// Return the bucket info
	api.Created(w, bucket)
}
