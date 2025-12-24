package http

import (
	"errors"
	"net/http"
	"strconv"

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

	// Parse query parameter for including deleted ledgers
	includeDeleted := false
	if includeDeletedStr := r.URL.Query().Get("includeDeleted"); includeDeletedStr != "" {
		var err error
		includeDeleted, err = strconv.ParseBool(includeDeletedStr)
		if err != nil {
			api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", err)
			return
		}
	}

	ledgerInfo, err := s.cluster.GetLedgerInfo(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Filter out deleted ledgers unless includeDeleted is true
	if !includeDeleted && ledgerInfo.DeletedAt != nil {
		api.WriteErrorResponse(w, http.StatusNotFound, "NOT_FOUND", errors.New("ledger not found"))
		return
	}

	// Return ledger info
	api.Ok(w, LedgerResponse{
		LedgerInfo: ledgerInfo,
	})
}
