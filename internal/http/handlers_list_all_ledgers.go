package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// handleListAllLedgers handles GET /ledgers to list all ledgers across all buckets
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {

	// Get all buckets info
	bucketsInfo := s.cluster.GetAllBucketsInfo(r.Context())

	// Collect all ledgers with their bucket names
	ledgersList := make([]LedgerResponse, 0)
	for bucketName, bucketInfo := range bucketsInfo {
		// Get the bucket cluster to access ledgers
		bucket, err := s.cluster.GetBucketCluster(r.Context(), bucketName)
		if err != nil {
			s.logger.Infof("WARN: Failed to get bucket cluster for '%s': %v", bucketName, err)
			continue
		}
		ledgers, err := bucket.GetLedgers(r.Context())
		if err != nil {
			s.logger.Infof("WARN: Failed to get ledgers from bucket '%s': %v", bucketName, err)
			continue
		}
		for _, ledgerInfo := range ledgers {
			ledgersList = append(ledgersList, LedgerResponse{
				LedgerInfo: ledgerInfo,
				Bucket:     bucketInfo.Name,
			})
		}
	}

	api.Ok(w, ledgersList)
}
