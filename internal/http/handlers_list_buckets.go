package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// handleListBuckets handles GET /buckets to list all buckets
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	// Get all buckets from cluster
	buckets := s.cluster.GetAllBucketsInfo(r.Context())

	// Convert map to slice
	bucketsList := make([]ledger.BucketInfo, 0, len(buckets))
	for _, bucketInfo := range buckets {
		bucketsList = append(bucketsList, bucketInfo)
	}

	api.Ok(w, bucketsList)
}
