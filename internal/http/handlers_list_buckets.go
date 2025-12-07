package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal"
)

// handleListBuckets handles GET /buckets to list all buckets
func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	// Get all buckets from cluster
	buckets := s.cluster.GetAllBuckets()

	// Convert map to slice
	bucketsList := make([]ledger.BucketInfo, 0, len(buckets))
	for _, bucket := range buckets {
		bucketsList = append(bucketsList, bucket.Info())
	}

	api.Ok(w, bucketsList)
}
