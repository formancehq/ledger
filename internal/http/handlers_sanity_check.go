package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// SanityCheckResult represents the result of a sanity check
type SanityCheckResult struct {
	Status string `json:"status"`
}

// handleSanityCheck handles GET /{ledgerName}/sanity-check to verify local storage validity
// This endpoint checks the local node storage only, without forwarding to the leader
func (s *Server) handleSanityCheck(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	// TODO: Implement sanity check logic
	// This should verify the local storage validity without gRPC calls to the leader

	writeOK(w, SanityCheckResult{
		Status: "not_implemented",
	})
}
