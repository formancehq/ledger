package http

import (
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/api"
)

// handleListAllLedgers handles GET / to list all ledgers
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	// Parse query parameter for including deleted ledgers
	includeDeleted := false
	if includeDeletedStr := r.URL.Query().Get("includeDeleted"); includeDeletedStr != "" {
		var err error
		includeDeleted, err = strconv.ParseBool(includeDeletedStr)
		if err != nil {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", err)
			return
		}
	}

	// Get all ledgers info
	ledgersInfo := s.cluster.GetAllLedgersInfo(r.Context())

	// Convert to response format
	ledgersList := make([]LedgerResponse, 0, len(ledgersInfo))
	for _, ledgerInfo := range ledgersInfo {
		// Filter out deleted ledgers unless includeDeleted is true
		if !includeDeleted && ledgerInfo.DeletedAt != nil {
			continue
		}
		ledgersList = append(ledgersList, LedgerResponse{
			LedgerInfo: ledgerInfo,
		})
	}

	api.Ok(w, ledgersList)
}
