package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetLedger handles GET /{ledgerName} to get a ledger
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerInfo, err := s.backend.GetLedgerInfo(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return ledger info wrapped in BaseResponse
	writeOK(w, ledgerInfo)
}
