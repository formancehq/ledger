package http

import (
	"net/http"

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

	address, ok := requirePathParameter(w, r, "address", "account address")
	if !ok {
		return
	}

	// Verify ledger exists
	_, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	opts := ctrl.GetAccountOptions{
		CollapseColors: queryParamBool(r, "collapseColors"),
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

	writeMonetaryOK(w, r, account)
}
