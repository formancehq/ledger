package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/readprojection"
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

	// Return ledgers list wrapped in BaseResponse
	for i, info := range ret {
		ret[i] = readprojection.Ledger(info)
	}
	writeOK(w, ret)
}
