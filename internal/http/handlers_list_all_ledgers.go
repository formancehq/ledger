package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// handleListAllLedgers handles GET /ledgers to list all ledgers across all buckets
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {

	// Get all buckets
	buckets := s.cluster.GetAllBuckets()

	// Collect all ledgers with their bucket names
	ledgersList := make([]LedgerResponse, 0)
	for _, bucket := range buckets {
		ledgers, err := bucket.GetLedgers(r.Context())
		if err != nil {
			s.logger.Infof("WARN: Failed to get ledgers from bucket")
			continue
		}
		for _, ledgerInfo := range ledgers {
			ledgersList = append(ledgersList, LedgerResponse{
				LedgerInfo: ledgerInfo,
				Bucket:     bucket.Info().Name,
			})
		}
	}

	api.Ok(w, ledgersList)
}

