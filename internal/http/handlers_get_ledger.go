package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/go-chi/chi/v5"
)

// handleGetLedger handles GET /{ledgerName} to get a ledger
func (s *Server) handleGetLedger(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerInfo, err := s.cluster.GetLedgerInfo(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Return ledger info
	api.Ok(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
	})
}
