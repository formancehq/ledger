package http

import (
	"net/http"
)

// handleListPreparedQueries handles GET /{ledgerName}/prepared-queries.
func (s *Server) handleListPreparedQueries(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queries, err := s.backend.ListPreparedQueries(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, queries)
}
