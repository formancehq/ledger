package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// handleListAllLedgers handles GET / to list all ledgers
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	// Get all ledgers info
	ledgersInfo, err := s.backend.GetAllLedgersInfo(r.Context())
	if err != nil {
		handleError(w, r, err)
		return
	}

	ret := make([]*commonpb.LedgerInfo, len(ledgersInfo))
	i := 0
	for _, l := range ledgersInfo {
		ret[i] = l
		i++
	}

	// Return ledgers list wrapped in BaseResponse
	writeOK(w, ret)
}
