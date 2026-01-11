package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleDeleteLedger handles DELETE /{ledgerName} to delete a ledger
func (s *Server) handleDeleteLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	err := s.backend.DeleteLedger(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return 204 No Content on successful deletion
	w.WriteHeader(http.StatusNoContent)
}
