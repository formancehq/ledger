package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
)

// handleGetAccount handles GET /{ledgerName}/accounts/{address} to retrieve an account.
// The optional ?collapseColors=true query param sums every colored bucket of
// the same asset into a single entry with color="" in the response.
func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
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

	opts := ctrl.GetAccountOptions{
		CollapseColors: r.URL.Query().Get("collapseColors") == "true",
	}

	account, err := s.backend.GetAccount(r.Context(), ledgerName, address, opts)
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
