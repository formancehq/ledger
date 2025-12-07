package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// handleListAllLedgers handles GET /ledgers to list all ledgers across all buckets
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get all buckets
	buckets := s.cluster.GetAllBuckets()

	// Collect all ledgers with their bucket names
	ledgersList := make([]LedgerResponse, 0)
	for bucketName := range buckets {
		ledgers, err := s.cluster.GetAllLedgers(bucketName)
		if err != nil {
			s.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Infof("WARN: Failed to get ledgers from bucket")
			continue
		}
		for _, ledgerInfo := range ledgers {
			ledgersList = append(ledgersList, LedgerResponse{
				LedgerInfo: ledgerInfo,
				Bucket:     bucketName,
			})
		}
	}

	api.Ok(w, ledgersList)
}

