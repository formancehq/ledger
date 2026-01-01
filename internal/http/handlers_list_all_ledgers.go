package http

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
)

// handleListAllLedgers handles GET / to list all ledgers
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	// Get all ledgers info
	ledgersInfo, err := s.cluster.GetAllLedgersInfo(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Convert to response format
	ledgersList := make([]LedgerResponse, 0, len(ledgersInfo))
	for _, ledgerInfo := range ledgersInfo {
		ledgersList = append(ledgersList, LedgerResponse{
			LedgerInfo: ledgerInfo,
		})
	}

	api.Ok(w, ledgersList)
}
