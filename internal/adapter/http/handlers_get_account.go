package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// handleGetAccount handles GET /{ledgerName}/accounts/{address} to retrieve an account.
//
// Per-asset volumes are populated by the controller's `scanAccount` on every
// read (Pebble prefix scan on the volume-canonical range), so the response
// carries them by default — matching v2's inline `volumes`. This handler adds
// no extra query. The wire shape is driven by `Account.MarshalJSON`, which is
// the piece that used to drop the field.
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

	account, err := s.backend.GetAccount(r.Context(), ledgerName, address)
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
