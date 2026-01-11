package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// handleListAllLedgers handles GET / to list all ledgers
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	// Get all ledgers info
	ledgersInfo, err := s.backend.GetAllLedgers(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}

	ret := make([]*ledgerpb.LedgerInfo, 0, len(ledgersInfo))
	for _, ledger := range ledgersInfo {
		ret = append(ret, ledger)
	}

	// Return ledgers list wrapped in BaseResponse
	writeOK(w, ret)
}
