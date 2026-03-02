package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleListPreparedQueries handles GET /{ledgerName}/prepared-queries
func (s *Server) handleListPreparedQueries(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	queries, err := s.backend.ListPreparedQueries(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	writeOK(w, queries)
}
