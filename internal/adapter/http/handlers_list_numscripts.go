package http

import (
	"net/http"
)

// handleListNumscripts handles GET /{ledgerName}/numscripts to list the greatest
// version of every numscript for a ledger.
func (s *Server) handleListNumscripts(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	scripts, err := s.backend.ListNumscripts(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, scripts)
}
