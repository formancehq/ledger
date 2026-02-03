package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetAccount handles GET /{ledgerName}/accounts/{address} to retrieve an account
func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	address := chi.URLParam(r, "address")
	if address == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("account address is required"))
		return
	}

	// Verify ledger exists
	_, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	account, err := s.backend.GetAccount(r.Context(), ledgerName, address)
	if err != nil {
		s.logger.WithFields(map[string]any{
			"ledger":  ledgerName,
			"address": address,
			"error":   err,
		}).Errorf("Failed to get account")
		handleError(w, r, err)
		return
	}

	writeOK(w, account)
}
