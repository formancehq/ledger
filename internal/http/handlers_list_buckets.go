package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// handleListBuckets handles GET /buckets to list all buckets
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		api.WriteErrorResponse(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get all buckets from cluster
	buckets := s.cluster.GetAllBuckets()

	// Convert map to slice
	bucketsList := make([]service.BucketInfo, 0, len(buckets))
	for _, bucket := range buckets {
		bucketsList = append(bucketsList, bucket)
	}

	api.Ok(w, bucketsList)
}

