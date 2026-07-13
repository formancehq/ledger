package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetIndexStatus handles GET /indexes/status to fetch the aggregated
// index registry status. Query parameter `ledger` (optional) narrows the
// per-index entries to a single ledger; the aggregated counters
// (LastIndexedSequence, LastLogSequence, Lag, IndexFileSize) always cover
// the bucket regardless.
func (s *Server) handleGetIndexStatus(w http.ResponseWriter, r *http.Request) {
	req := &servicepb.GetIndexStatusRequest{
		Ledger: r.URL.Query().Get("ledger"),
	}

	resp, err := s.backend.GetIndexStatus(r.Context(), req)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeProtoOK(w, resp)
}
