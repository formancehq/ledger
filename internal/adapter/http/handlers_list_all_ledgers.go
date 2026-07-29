package http

import (
	"net/http"

	adapterledgerinfo "github.com/formancehq/ledger/v3/internal/adapter/ledgerinfo"
)

// handleListAllLedgers handles GET / to list all ledgers.
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get all ledgers info
	cursor, err := s.backend.ListLedgers(ctx)
	if err != nil {
		handleError(w, r, err)

		return
	}

	ret, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	for i, ledgerInfo := range ret {
		ret[i] = adapterledgerinfo.RedactSecrets(ledgerInfo)
	}

	// Return ledgers list wrapped in BaseResponse
	writeOK(w, ret)
}
