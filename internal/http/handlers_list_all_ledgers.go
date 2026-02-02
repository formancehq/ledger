package http

import (
	"io"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// handleListAllLedgers handles GET / to list all ledgers
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get all ledgers info
	cursor, err := s.backend.GetAllLedgersInfo(ctx)
	if err != nil {
		handleError(w, r, err)
		return
	}
	defer func() {
		_ = cursor.Close()
	}()

	var ret []*commonpb.LedgerInfo
	for {
		ledger, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			handleError(w, r, err)
			return
		}
		ret = append(ret, ledger)
	}

	// Return ledgers list wrapped in BaseResponse
	writeOK(w, ret)
}
