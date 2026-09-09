package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/readprojection"
)

// handleGetLedger handles GET /{ledgerName} to get a ledger.
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	// Return ledger info wrapped in BaseResponse
	writeOK(w, readprojection.Ledger(ledgerInfo))
}
