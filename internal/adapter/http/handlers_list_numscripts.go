package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleListNumscripts handles GET /{ledgerName}/numscripts to list all numscripts for a ledger.
func (s *Server) handleListNumscripts(w http.ResponseWriter, r *http.Request) {
	ledger := chi.URLParam(r, "ledgerName")

	scripts, err := s.backend.ListNumscripts(r.Context(), ledger)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, scripts)
}
