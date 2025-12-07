package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleGetLedger handles GET /ledgers/{ledgerName} to get a ledger
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	if s.cluster == nil {
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", errors.New("cluster not available"))
		return
	}

	// Get ledger by name (finds bucket automatically)
	ledgerInfo, bucketName, exists, err := s.cluster.GetLedgerByName(ledgerName)
	if err != nil {
		s.logger.WithFields(map[string]any{"name": ledgerName, "error": err}).Errorf("Failed to get ledger")
		api.InternalServerError(w, r, err)
		return
	}

	if !exists {
		api.WriteErrorResponse(w, http.StatusNotFound, "LEDGER_NOT_FOUND", fmt.Errorf("ledger %s not found", ledgerName))
		return
	}

	// Return ledger info with bucket name
	api.Ok(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
		Bucket:     bucketName,
	})
}

